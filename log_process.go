package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client"
)

type LogProces struct {
	rc     chan []byte
	wc     chan *Message
	read   Reader
	writer Writer
}

type Reader interface {
	Read(rc chan []byte)
}
type Writer interface {
	Write(wc chan *Message)
}

// 读取模块

type readFromFile struct {
	path string
}

func (r *readFromFile) Read(rc chan []byte) {
	// 打开文件
	f, err := os.Open(r.path)
	// 文件指针移动到末尾
	f.Seek(0, 2)
	if err != nil {
		panic(err)
	}
	rd := bufio.NewReader(f)
	// 读取知道换行符为止
	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Microsecond)
			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error :%s", err.Error()))
		}
		TypeMonitorChan <- TypeHandleLine
		rc <- line[:len(line)-1]
	}

}

// 写入模块
type WriterToDb struct {
	dbsn string //data source
}
type Message struct {
	TimeLocal                    time.Time
	BytesSent                    int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

func (w *WriterToDb) Write(wc chan *Message) {
	// 写入模块

	infSli := strings.Split(w.dbsn, "@")

	// Create a new HTTPClient
	//c, err := client.NewClient(client.HTTPConfig{
	//	Addr:     infSli[0],
	//	Username: infSli[1],
	//	Password: infSli[2],
	//})
	host, err := url.Parse(infSli[0])
	// NOTE: this assumes you've setup a user and have setup shell env variables,
	// namely INFLUX_USER/INFLUX_PWD. If not just omit Username/Password below.
	//fmt.Println(infSli[1])
	///fmt.Println(host)
	conf := client.Config{
		URL:      *host,
		Username: infSli[1],
		Password: infSli[2],
	}
	c, err := client.NewClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	for v := range wc {
		pts := make([]client.Point, 1)
		pts[0] = client.Point{
			Measurement: "logs",
			Tags:        map[string]string{"Path": v.Path, "Method": v.Method, "Scheme": v.Scheme, "Status": v.Status},
			Fields: map[string]interface{}{
				"UpstreamTime": v.UpstreamTime,
				"RequestTime":  v.RequestTime,
				"BytesSent":    v.BytesSent,
			},
			Time:      time.Now(),
			Precision: "s",
		}

		bps := client.BatchPoints{
			Points:   pts,
			Database: "log_access", // 数据库
			// RetentionPolicy: "default",
		}
		_, err = c.Write(bps)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("write success!")
	}

}

// 解析模块
func (l *LogProces) Process() {
	// 解析模块

	/**
	172.0.0.12 - - [04/Mar/2018:13:49:52 +0000] http "GET /foo?query=t HTTP/1.0" 200 2133 "-" "KeepAliveClient" "-" 1.005 1.854
	*/

	r := regexp.MustCompile(`([\d\.]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range l.rc {
		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Println("FindStringSubmatch fail:", string(v))
			continue
		}

		message := &Message{}
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
			continue
		}
		message.TimeLocal = t

		byteSent, _ := strconv.Atoi(ret[8])
		message.BytesSent = byteSent

		// GET /foo?query=t HTTP/1.0
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Println("strings.Split fail", ret[6])
			continue
		}
		message.Method = reqSli[0]

		u, err := url.Parse(reqSli[1])
		if err != nil {
			log.Println("url parse fail:", err)
			TypeMonitorChan <- TypeErrNum
			continue
		}
		message.Path = u.Path

		message.Scheme = ret[5]
		message.Status = ret[7]

		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime

		l.wc <- message
	}
}

// 系统状态监控
type SystemInfo struct {
	HandleLine   int     `json:"handleLine"`   // 总处理日志行数
	Tps          float64 `json:"tps"`          // 系统吞出量
	ReadChanLen  int     `json:"readChanLen"`  // read channel 长度
	WriteChanLen int     `json:"writeChanLen"` // write channel 长度
	RunTime      string  `json:"runTime"`      // 运行总时间
	ErrNum       int     `json:"errNum"`       // 错误数
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var TypeMonitorChan = make(chan int, 200)

type Monitor struct {
	startTime time.Time
	data      SystemInfo
	tpsSli    []int
}

func (m *Monitor) start(lp *LogProces) {

	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.data.ErrNum += 1
			case TypeHandleLine:
				m.data.HandleLine += 1
			}
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			<-ticker.C
			m.tpsSli = append(m.tpsSli, m.data.HandleLine)
			if len(m.tpsSli) > 2 {
				m.tpsSli = m.tpsSli[1:]
			}
		}
	}()

	http.HandleFunc("/  ", func(writer http.ResponseWriter, request *http.Request) {
		m.data.RunTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(lp.rc)
		m.data.WriteChanLen = len(lp.wc)

		if len(m.tpsSli) >= 2 {
			m.data.Tps = float64(m.tpsSli[1]-m.tpsSli[0]) / 5
		}

		ret, _ := json.MarshalIndent(m.data, "", "\t")
		io.WriteString(writer, string(ret))
	})

	http.ListenAndServe(":9193", nil)
}
func main() {
	var path, dbsn string
	flag.StringVar(&path, "path", "./access.log", "read file path")
	flag.StringVar(&dbsn, "influxDsn", "http://127.0.0.1:8086@username@password@imooc@s", "influx data source")
	flag.Parse()

	r := &readFromFile{
		path: path,
	}
	w := &WriterToDb{
		dbsn: dbsn,
	}
	lp := &LogProces{
		rc:     make(chan []byte, 200),
		wc:     make(chan *Message, 200),
		read:   r,
		writer: w,
	}
	go lp.read.Read(lp.rc)
	for i := 0; i < 2; i++ {
		go lp.Process()
	}
	for i := 0; i < 4;  ++ {
		go lp.writer.Write(lp.wc)
	}

	m := &Monitor{
		startTime: time.Now(),
		data:      SystemInfo{},
	}
	m.start(lp)
	time.Sleep(300 * time.Second)

}
