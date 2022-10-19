package debug

import (
	"fmt"
	"io"
	"os"
	"time"
)

var (
	debugStart time.Time
	debug      = 2

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
	//oFile, _ = os.Create("output.log")
	oFile = os.Stdout
}

func Debug(topic logTopic, format string, a ...interface{}) {

	if debug >= 1 {
		t := time.Since(debugStart).Microseconds()
		t /= 100
		prefix := fmt.Sprintf("%06d %v ", t, string(topic))
		format = prefix + format
		fmt.Fprintf(oFile, format, a...)
	}
}
