package main

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"flag"
	"fmt"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/pkg/errors"
	"github.com/taosdata/driver-go/v2/af/param"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_load"
	"github.com/influxdata/influxdb-comparisons/util/report"

	"github.com/taosdata/driver-go/v2/af"
)

// TODO AP: Maybe useless
const RateControlGranularity = 1000 // 1000 ms = 1s
const RateControlMinBatchSize = 100

type TDengineBulkLoad struct {
	conn     *af.Connector
	hostname string
	username string
	password string
	port     int

	ingestRateLimit int
	backoff         time.Duration
	backoffTimeOut  time.Duration

	//runtime vars
	bufPool               sync.Pool
	batchChan             chan batch
	inputDone             chan struct{}
	progressIntervalItems uint64
	ingestionRateGran     float64
	maxBatchSize          int
	speedUpRequest        int32
	scanFinished          bool
	totalBackOffSecs      float64
	configs               []*workerConfig
	valuesRead            int64
	itemsRead             int64
	bytesRead             int64
}

type batch struct {
	Buffer *bytes.Buffer
	Items  int
	Values int
}

var load = &TDengineBulkLoad{}

// Parse args:
func init() {
	bulk_load.Runner.Init(5000)
	load.Init()

	flag.Parse()

	bulk_load.Runner.Validate()
	load.Validate()

}

func main() {
	bulk_load.Runner.Run(load)
}

type workerConfig struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	writer         *TaosWriter
	backingOffSecs float64
}

func (t *TDengineBulkLoad) Init() {
	flag.StringVar(&t.hostname, "hostname", "localhost", "hostname")
	flag.IntVar(&t.port, "port", 6030, "port")
	flag.StringVar(&t.username, "username", "", "username")
	flag.StringVar(&t.password, "password", "", "password")
	flag.DurationVar(&t.backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.DurationVar(&t.backoffTimeOut, "backoff-timeout", time.Minute*30, "Maximum time to spent when dealing with backoff messages in one shot")
	flag.IntVar(&t.ingestRateLimit, "ingest-rate-limit", -1, "Ingest rate limit in values/s (-1 = no limit).")
}

func (t *TDengineBulkLoad) Validate() {

	if t.ingestRateLimit > 0 {
		t.ingestionRateGran = (float64(t.ingestRateLimit) / float64(bulk_load.Runner.Workers)) / (float64(1000) / float64(RateControlGranularity))
		log.Printf("Using worker ingestion rate %v values/%v ms", t.ingestionRateGran, RateControlGranularity)
		recommendedBatchSize := int((t.ingestionRateGran / bulk_load.ValuesPerMeasurement) * 0.20)
		log.Printf("Calculated batch size hint: %v (allowed min: %v max: %v)", recommendedBatchSize, RateControlMinBatchSize, bulk_load.Runner.BatchSize)
		if recommendedBatchSize < RateControlMinBatchSize {
			recommendedBatchSize = RateControlMinBatchSize
		} else if recommendedBatchSize > bulk_load.Runner.BatchSize {
			recommendedBatchSize = bulk_load.Runner.BatchSize
		}
		t.maxBatchSize = bulk_load.Runner.BatchSize
		if recommendedBatchSize != bulk_load.Runner.BatchSize {
			log.Printf("Adjusting batchSize from %v to %v (%v values in 1 batch)", bulk_load.Runner.BatchSize, recommendedBatchSize, float32(recommendedBatchSize)*bulk_load.ValuesPerMeasurement)
			bulk_load.Runner.BatchSize = recommendedBatchSize
		}
	} else {
		log.Print("Ingestion rate control is off")
	}

	if bulk_load.Runner.TimeLimit > 0 && t.backoffTimeOut > bulk_load.Runner.TimeLimit {
		t.backoffTimeOut = bulk_load.Runner.TimeLimit
	}
}

func (t *TDengineBulkLoad) RunScanner(r io.Reader, syncChanDone chan int) {
	t.scanFinished = false
	t.itemsRead = 0
	t.bytesRead = 0
	t.valuesRead = 0
	buf := t.bufPool.Get().(*bytes.Buffer)

	var n, values int
	var totalPoints, totalValues, totalValuesCounted int64

	newline := []byte("\n")
	var deadline time.Time
	if bulk_load.Runner.TimeLimit > 0 {
		deadline = time.Now().Add(bulk_load.Runner.TimeLimit)
	}

	var batchItemCount uint64
	var err error
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 4*1024*1024))
outer:
	for scanner.Scan() {
		if t.itemsRead == bulk_load.Runner.ItemLimit {
			break
		}

		line := scanner.Text()
		totalPoints, totalValues, err = common.CheckTotalValues(line)
		if totalPoints > 0 || totalValues > 0 {
			continue
		} else {
			fieldCnt := countFields(line)
			values += fieldCnt
			totalValuesCounted += int64(fieldCnt)
		}
		if err != nil {
			log.Fatal(err)
		}
		t.itemsRead++
		batchItemCount++

		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		if n >= bulk_load.Runner.BatchSize {
			atomic.AddUint64(&t.progressIntervalItems, batchItemCount)
			batchItemCount = 0

			t.bytesRead += int64(buf.Len())
			t.batchChan <- batch{buf, n, values}
			buf = t.bufPool.Get().(*bytes.Buffer)
			n = 0
			values = 0

			if bulk_load.Runner.TimeLimit > 0 && time.Now().After(deadline) {
				bulk_load.Runner.SetPrematureEnd("Timeout elapsed")
				break outer
			}

			if t.ingestRateLimit > 0 {
				if bulk_load.Runner.BatchSize < t.maxBatchSize {
					hint := atomic.LoadInt32(&t.speedUpRequest)
					if hint > int32(bulk_load.Runner.Workers*2) { // we should wait for more requests (and this is just a magic number)
						atomic.StoreInt32(&t.speedUpRequest, 0)
						bulk_load.Runner.BatchSize += int(float32(t.maxBatchSize) * 0.10)
						if bulk_load.Runner.BatchSize > t.maxBatchSize {
							bulk_load.Runner.BatchSize = t.maxBatchSize
						}
						log.Printf("Increased batch size to %d\n", bulk_load.Runner.BatchSize)
					}
				}
			}
		}
		select {
		case <-syncChanDone:
			break outer
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		t.batchChan <- batch{buf, n, values}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(t.inputDone)

	t.valuesRead = totalValues
	if totalValues == 0 {
		t.valuesRead = totalValuesCounted
	}
	if t.itemsRead != totalPoints { // totalPoints is unknown (0) when exiting prematurely due to time limit
		if !bulk_load.Runner.HasEndedPrematurely() {
			log.Fatalf("Incorrent number of read points: %d, expected: %d:", t.itemsRead, totalPoints)
		}
	}
	t.scanFinished = true
}

func (t *TDengineBulkLoad) IsScanFinished() bool {
	return t.scanFinished
}

func (t *TDengineBulkLoad) GetReadStatistics() (itemsRead, bytesRead, valuesRead int64) {
	itemsRead = t.itemsRead
	bytesRead = t.bytesRead
	valuesRead = t.valuesRead
	return
}

func (t *TDengineBulkLoad) CreateDb() {
	var err error
	t.conn, err = af.Open(t.hostname, t.username, t.password, "", t.port)
	if err != nil {
		log.Fatalf("fail to connect, err: %s", err)
	}

	existingDatabases, err := t.listDatabases()
	if err != nil {
		log.Fatal(err)
	}

	delete(existingDatabases, "log")
	if len(existingDatabases) > 0 {
		var dbs []string
		for key, _ := range existingDatabases {
			dbs = append(dbs, key)
		}
		dbs_string := strings.Join(dbs, ", ")
		if bulk_load.Runner.DoAbortOnExist {
			log.Fatalf("The following databases already exist in the data store: %s. ", dbs_string)
		} else {
			log.Printf("The following databases already exist in the data store: %s", dbs_string)
		}
	}

	_, ok := existingDatabases[bulk_load.Runner.DbName]
	if ok {
		log.Printf("Database [%s] already exists", bulk_load.Runner.DbName)
	} else {
		err = t.createDb(bulk_load.Runner.DbName)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
		log.Printf("Database [%s] created", bulk_load.Runner.DbName)
	}
}

func (t *TDengineBulkLoad) PrepareWorkers() {

	t.bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}

	t.batchChan = make(chan batch, bulk_load.Runner.Workers)
	t.inputDone = make(chan struct{})

	t.configs = make([]*workerConfig, bulk_load.Runner.Workers)
}

func (t *TDengineBulkLoad) GetBatchProcessor() bulk_load.BatchProcessor {
	return t
}

func (t *TDengineBulkLoad) GetScanner() bulk_load.Scanner {
	return t
}

func (t *TDengineBulkLoad) SyncEnd() {
	<-t.inputDone
	close(t.batchChan)
}

func (t *TDengineBulkLoad) CleanUp() {
	for _, c := range t.configs {
		close(c.backingOffChan)
		<-c.backingOffDone
	}
	t.totalBackOffSecs = float64(0)
	for i := 0; i < bulk_load.Runner.Workers; i++ {
		t.totalBackOffSecs += t.configs[i].backingOffSecs
	}
}

func (t *TDengineBulkLoad) UpdateReport(params *report.LoadReportParams) (reportTags [][2]string, extraVals []report.ExtraVal) {
	reportTags = [][2]string{{"back_off", strconv.Itoa(int(t.backoff.Seconds()))}}
	extraVals = make([]report.ExtraVal, 0)

	if t.ingestRateLimit > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "ingest_rate_limit_values", Value: t.ingestRateLimit})
	}
	if t.totalBackOffSecs > 0 {
		extraVals = append(extraVals, report.ExtraVal{Name: "total_backoff_secs", Value: t.totalBackOffSecs})
	}

	params.DBType = "TDengine"

	return
}

type TaosConfig struct {
	hostname string
	username string
	password string
	port     int
	database string

	BackingOffChan chan bool
	BackingOffDone chan struct{}

	// Debug label for more informative errors.
	DebugInfo string
}

func (t *TDengineBulkLoad) PrepareProcess(i int) {
	t.configs[i] = &workerConfig{
		backingOffChan: make(chan bool, 100),
		backingOffDone: make(chan struct{}),
	}

	c := &TaosConfig{
		hostname:       t.hostname,
		username:       t.username,
		password:       t.password,
		port:           t.port,
		database:       bulk_load.Runner.DbName,
		BackingOffChan: t.configs[i].backingOffChan,
		BackingOffDone: t.configs[i].backingOffDone,
		DebugInfo:      "",
	}

	t.configs[i].writer = NewTaosWriter(*c)
}

func (t *TDengineBulkLoad) RunProcess(i int, waitGroup *sync.WaitGroup, telemetryPoints chan *report.Point, reportTags [][2]string) error {
	return t.processBatches(t.configs[i].writer, t.configs[i].backingOffChan, telemetryPoints, fmt.Sprintf("%d", i), waitGroup, reportTags)
}

func (t *TDengineBulkLoad) AfterRunProcess(i int) {
	t.configs[i].backingOffSecs = processBackoffMessages(i, t.configs[i].backingOffChan, t.configs[i].backingOffDone)
}

func (t TDengineBulkLoad) EmptyBatchChanel() {
	for range t.batchChan {
		//read out remaining batches
	}
}

func (t *TDengineBulkLoad) listDatabases() (map[string]string, error) {
	rows, err := t.conn.StmtQuery("SHOW DATABASES", param.NewParam(0))
	if err != nil {
		return nil, errors.Errorf("show database failed: %s", err)
	}

	var databases = make(map[string]string, 0)
	var result = make([]driver.Value, 10)
	for true {
		if err := rows.Next(result); err != nil {
			return databases, nil
		}

		databases[fmt.Sprintf("%v", result[0])] = ""
		return databases, nil
	}
	return nil, errors.New("unknown error")
}

func (t *TDengineBulkLoad) createDb(db string) error {
	_, _ = t.conn.Exec(fmt.Sprintf("CREATE DATABASE %s", db))

	_, err := t.conn.Exec(fmt.Sprintf("USE %s", db))

	if err != nil {
		errors.Errorf("use database %s field", db)
	}

	return nil
}

// countFields return number of fields in protocol line
func countFields(line string) int {
	lineParts := strings.Split(line, " ") // "measurement,tags fields timestamp"
	if len(lineParts) != 3 {
		log.Fatalf("invalid protocol line: '%s'", line)
	}
	fieldCnt := strings.Count(lineParts[1], "=")
	if fieldCnt == 0 {
		log.Fatalf("invalid fields parts: '%s'", lineParts[1])
	}
	return fieldCnt
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func (t *TDengineBulkLoad) processBatches(w *TaosWriter, backoffSrc chan bool, telemetrySink chan *report.Point, telemetryWorkerLabel string, workersGroup *sync.WaitGroup, reportTags [][2]string) error {
	var batchesSeen int64

	// Ingestion rate control vars
	var gvCount float64
	var gvStart time.Time

	defer workersGroup.Done()

	for batch := range t.batchChan {
		batchesSeen++

		//var bodySize int
		ts := time.Now().UnixNano()

		if t.ingestRateLimit > 0 && gvStart.Nanosecond() == 0 {
			gvStart = time.Now()
		}

		// Write the batch: try until backoff is not needed.
		if bulk_load.Runner.DoLoad {
			var err error
			sleepTime := t.backoff
			timeStart := time.Now()
			for {
				_, err = w.WriteLineProtocol(batch.Buffer.Bytes(), "ns")
				if err != nil {
					backoffSrc <- true
					// Report telemetry, if applicable:
					if telemetrySink != nil {
						p := report.GetPointFromGlobalPool()
						p.Init("benchmarks_telemetry", ts)
						for _, tagpair := range reportTags {
							p.AddTag(tagpair[0], tagpair[1])
						}
						p.AddTag("client_type", "load")
						p.AddTag("worker", telemetryWorkerLabel)
						p.AddBoolField("backoff", true)
						telemetrySink <- p
					}
					time.Sleep(sleepTime)
					sleepTime += t.backoff        // sleep longer if backpressure comes again
					if sleepTime > 10*t.backoff { // but not longer than 10x default backoff time
						log.Printf("[worker %s] sleeping on backoff response way too long (10x %v)", telemetryWorkerLabel, t.backoff)
						sleepTime = 10 * t.backoff
					}
					checkTime := time.Now()
					if timeStart.Add(t.backoffTimeOut).Before(checkTime) {
						log.Printf("[worker %s] Spent too much time in backoff: %ds\n", telemetryWorkerLabel, int64(checkTime.Sub(timeStart).Seconds()))
						break
					}
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				return fmt.Errorf("Error writing: %s\n", err.Error())
			}
		}

		// lagMillis intentionally includes backoff time,
		// and incidentally includes compression time:
		lagMillis := float64(time.Now().UnixNano()-ts) / 1e6

		// Return the batch buffer to the pool.
		batch.Buffer.Reset()
		t.bufPool.Put(batch.Buffer)

		// Normally report after each batch
		reportStat := true
		valuesWritten := float64(batch.Values)

		// Apply ingest rate control if set
		if t.ingestRateLimit > 0 {
			gvCount += valuesWritten
			if gvCount >= t.ingestionRateGran {
				now := time.Now()
				elapsed := now.Sub(gvStart)
				overDelay := (gvCount - t.ingestionRateGran) / (t.ingestionRateGran / float64(RateControlGranularity))
				remainingMs := RateControlGranularity - (elapsed.Nanoseconds() / 1e6) + int64(overDelay)
				valuesWritten = gvCount
				lagMillis = float64(elapsed.Nanoseconds() / 1e6)
				if remainingMs > 0 {
					time.Sleep(time.Duration(remainingMs) * time.Millisecond)
					gvStart = time.Now()
					realDelay := gvStart.Sub(now).Nanoseconds() / 1e6 // 'now' was before sleep
					lagMillis += float64(realDelay)
				} else {
					gvStart = now
					atomic.AddInt32(&t.speedUpRequest, 1)
				}
				gvCount = 0
			} else {
				reportStat = false
			}
		}

		// Report sent batch statistic
		if reportStat {
			stat := bulk_load.Runner.StatPool.Get().(*bulk_load.Stat)
			stat.Label = []byte(telemetryWorkerLabel)
			stat.Value = valuesWritten
			bulk_load.Runner.StatChan <- stat
		}
	}

	return nil
}

func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) float64 {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			log.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	log.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
	return totalBackoffSecs
}

type TaosWriter struct {
	conn *af.Connector

	config *TaosConfig

	// Debug label for more informative errors.
	DebugInfo string
}

func (w *TaosWriter) WriteLineProtocol(batchLines []byte, precision string) (int64, error) {
	batchLineSlice := bytes.Split(batchLines, []byte("\n"))
	var lines []string
	for i := 0; i < len(batchLineSlice)-1; i++ {
		lines = append(lines, string(batchLineSlice[i]))
	}

	start := time.Now()
	if err := w.conn.InfluxDBInsertLines(lines, precision); err != nil {
		fmt.Println(err)
		return 0, err
	}

	lat := time.Since(start).Nanoseconds()

	return lat, nil

}

func NewTaosWriter(c TaosConfig) *TaosWriter {
	conn, _ := af.Open(c.hostname, c.username, c.password, c.database, c.port)
	return &TaosWriter{
		conn:      conn,
		config:    &c,
		DebugInfo: "",
	}

}
