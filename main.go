package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"sync/atomic"
	"github.com/levigross/grequests"
	"math/rand"
	"fmt"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	. "github.com/onestack/cron-room/helper"
	"go.uber.org/zap"
	"runtime"
)

var (
	urlsCrawled uint64

	crawlLimit    	  uint64 = 1
	numThreads        uint64 = 100
	crawlDelaySeconds uint64 = 0

	crawlWaitGroup sync.WaitGroup
	requestQueue   = make(chan *RequestEntity, 10)
	responseQueue  = make(chan *ResponseEntity, 15)
	shutdownNotify = make(chan struct{})
	statusRequest  = make(chan os.Signal, 1)

	logger *zap.Logger = CreateLogger("crawler")
)

type RequestEntity struct {
	Tag            string
	URL            string
	RequestOptions *grequests.RequestOptions
}

type ResponseEntity struct {
	Tag  string
	Resp *grequests.Response
}

func status() {
	for {
		select {
		case <-statusRequest:
			writeInfo("status", fmt.Sprintf("RequestQueue len:%d, ResponseQueue len:%d, URLs crawled:%d", len(requestQueue), len(responseQueue), atomic.LoadUint64(&urlsCrawled)))
		case <-shutdownNotify:
			writeInfo("status", "<-shutdownNotify")
			signal.Stop(statusRequest)
			return
		}
	}
}

func spiders() {
	crawlWaitGroup.Add(1)
	for {
		select {
		case requestEntity := <-requestQueue:
			time.Sleep(time.Second * time.Duration(crawlDelaySeconds))
			resp, err := grequests.Get(requestEntity.URL, requestEntity.RequestOptions)
			if err != nil {
				writeInfo("ResponseEntry", "Request error")
				continue
			}

			if !resp.Ok {
				writeInfo("ResponseEntry", "Response is not ok")
				continue
			}

			atomic.AddUint64(&urlsCrawled, 1)
			responseQueue <- CreateResponseEntity(requestEntity.Tag, resp)
		case <-shutdownNotify:
			crawlWaitGroup.Done()
			return

		}
	}
}

func eaters() {
	crawlWaitGroup.Add(1)
	for {
		select {
		case <-shutdownNotify:
			crawlWaitGroup.Done()
			return
		case responseEntity := <-responseQueue:
			//tag := responseEntry.Tag
			resp := responseEntity.Resp
			if atomic.LoadUint64(&urlsCrawled) == crawlLimit {
				log.Println("Crawl limit reached! Shutting down")
				resp.Close()
				close(shutdownNotify)
				continue
			}
			resp.Close()
		}

	}
}

func getUserAgent() string {
	var userAgent = [...]string{
		"Mozilla/5.0 (compatible, MSIE 10.0, Windows NT, DigExt)",
		"Mozilla/4.0 (compatible, MSIE 7.0, Windows NT 5.1, 360SE)",
		"Mozilla/4.0 (compatible, MSIE 8.0, Windows NT 6.0, Trident/4.0)",
		"Mozilla/5.0 (compatible, MSIE 9.0, Windows NT 6.1, Trident/5.0,",
		"Opera/9.80 (Windows NT 6.1, U, en) Presto/2.8.131 Version/11.11",
		"Mozilla/4.0 (compatible, MSIE 7.0, Windows NT 5.1, TencentTraveler 4.0)",
		"Mozilla/5.0 (Windows, U, Windows NT 6.1, en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
		"Mozilla/5.0 (Macintosh, Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
		"Mozilla/5.0 (Macintosh, U, Intel Mac OS X 10_6_8, en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
		"Mozilla/5.0 (Linux, U, Android 3.0, en-us, Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13",
		"Mozilla/5.0 (iPad, U, CPU OS 4_3_3 like Mac OS X, en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
		"Mozilla/4.0 (compatible, MSIE 7.0, Windows NT 5.1, Trident/4.0, SE 2.X MetaSr 1.0, SE 2.X MetaSr 1.0, .NET CLR 2.0.50727, SE 2.X MetaSr 1.0)",
		"Mozilla/5.0 (iPhone, U, CPU iPhone OS 4_3_3 like Mac OS X, en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5",
		"MQQBrowser/26 Mozilla/5.0 (Linux, U, Android 2.3.7, zh-cn, MB200 Build/GRJ22, CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
	}
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	return userAgent[r.Intn(len(userAgent))]
}

func CreateRequestEntity(tag string, requestURL string, params map[string]string) *RequestEntity {
	writeInfo("RequestEntity", fmt.Sprintf("Tag: %s, URL: %s", tag, requestURL))
	return &RequestEntity{
		Tag: tag,
		URL: requestURL,
		RequestOptions: &grequests.RequestOptions{
			UserAgent: getUserAgent(),
			Params: params,
		},
	}
}

func CreateResponseEntity(tag string, resp *grequests.Response) *ResponseEntity {
	writeInfo("ResponseEntry", fmt.Sprintf("Tag: %s, Resp: %v", tag, resp.String()))
	return &ResponseEntity{
		Tag: tag,
		Resp: resp,
	}
}

func CreateLogger(name string) *zap.Logger {
	filename := fmt.Sprintf("%s%s.log", "/tmp/cron-room-", name)
	ws := zapcore.AddSync(&lumberjack.Logger{
		Filename:   filename,
		MaxSize:    500, // megabytes
		MaxBackups: 3,   // backup
		MaxAge:     30,  // days
	})
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = ""
	coreInstance := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		ws,
		zap.InfoLevel,
	)
	return zap.New(coreInstance)
}

func writeInfo(msg string, data string) {
	logger.Info(msg,
		zap.String(LogKeySource, "[爬虫]"),
		zap.String(LogKeyTime, time.Now().Format(DateFullLayout)),
		zap.String(LogKeyData, data),
		zap.Int(LogKeyGoroutineNum, runtime.NumGoroutine()),
	)
}

func main() {
	signal.Notify(statusRequest, syscall.SIGHUP)
	go status()

	tag := "hizhu_houselist"
	requestURL := "http://m.hizhu.com/Home/House/houselist.html"
	params := map[string]string{
		"city_code": "001001",
		"pageno":    "1",
		"limit":     "1000",
		"sort":      "-1",
		"region_id": "",
		"plate_id": "",
		"money_max": "999999",
		"money_min": "0",
		"logicSort": "0",
		"line_id": "0",
		"stand_id": "0",
		"key": "0",
		"key_self": "0",
		"type_no": "0",
		"search_id": "0",
		"latitude": "0",
		"longitude": "0",
		"distance": "0",
		"update_time": "0",
	}
	requestQueue <- CreateRequestEntity(tag, requestURL, params)
	go eaters()

	for i := uint64(0); i < numThreads; i++ {
		go spiders()
	}
	<-shutdownNotify
	crawlWaitGroup.Wait()
}
