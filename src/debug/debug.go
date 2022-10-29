package debug

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var (
	debugStart time.Time
	debug      = 0

	oFile io.Writer
)

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DLog     logTopic = "LOG1"
	DLog2    logTopic = "LOG2"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DWarn    logTopic = "WARN"
)

func init() {
	debugStart = time.Now()
	os.RemoveAll("debug.log")
	oFile, _ = os.Create("debug.log")
	//oFile = os.Stdout
	log.SetOutput(oFile)
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {

	if debug >= 1 {
		t := time.Since(debugStart).Microseconds()
		t /= 100
		prefix := fmt.Sprintf("%06d %v ", t, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
