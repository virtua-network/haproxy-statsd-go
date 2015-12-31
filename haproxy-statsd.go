// Virtua HAproxy statistics emitter for statsd/graphite
// 
// Worflow :
//  - get HAproxy 1.5 CSV statistics via HTTP 
//  - send UDP statsd frames

package main

import (
    "encoding/csv"
    "encoding/json"
    "flag"
    "io"
    "log"
    "net/http"
    "os"
    "strings"
    "strconv"
    "time"

    // GOlang statsd implementation
    "gopkg.in/alexcesaro/statsd.v1"
)

// configuration options are defined here. Please see the provided config.json
// sample.
type Configuration struct {
    HAproxyUsername   string
    HAproxyPassword   string
    HAproxyUrl        string
    StatsdAddr        string
    StatsdPrefix      string
    SleepPeriod       time.Duration
}

// this is the core function : get HAproxy statistics via HTTP and send them to
// remote statsd.
func StatsEmitter(configuration Configuration) {
    // HAproxy HTTP statistics interface is usually protected by BASIC Auth
    cHttp := &http.Client{}
    req, err := http.NewRequest("GET", configuration.HAproxyUrl, nil)
    req.SetBasicAuth(configuration.HAproxyUsername, configuration.HAproxyPassword)
    resp, err := cHttp.Do(req)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()

    // CSV parser : skip the first line which contains definition
    reader := csv.NewReader(resp.Body)
    reader.Comment = '#'
    lineCount := 1 

    // open connection with statsd
    cStatsd, err := statsd.New(configuration.StatsdAddr,
                               statsd.WithPrefix(configuration.StatsdPrefix))
    if err != nil {
        panic(err)
    }

    for {
        record, err := reader.Read()
        if err == io.EOF {
            break
        } else if err != nil {
            panic(err)
        }

        // mapping of interresting CSV values (tested with HAproxy 1.5) 
        properties := make(map[string]string)
        properties["pxname"]   = record[0]
        properties["svname"]   = record[1]
        properties["scur"]     = record[4]
        properties["smax"]     = record[5]
        properties["bin"]      = record[8]
        properties["bout"]     = record[9]
        properties["ereq"]     = record[12]
        properties["econ"]     = record[13]
        properties["rate"]     = record[33]
        properties["hrsp_1xx"] = record[39]
        properties["hrsp_2xx"] = record[40]
        properties["hrsp_3xx"] = record[41]
        properties["hrsp_4xx"] = record[42]
        properties["hrsp_5xx"] = record[43]
        properties["qtime"]    = record[58]
        properties["ctime"]    = record[59]
        properties["rtime"]    = record[60]
        properties["ttime"]    = record[61]
         
        // as we are using uncommon naming convention in HAproxy proxy names,
        // replace ':' by '.' to avoid bad handling by graphite backend. 
        properties["pxname"] = strings.Replace(properties["pxname"], ":", ".", -1)

        // compute record for statsd
        for key, valueStr := range properties {
            // we don't want 'pxname' and 'svname' to be procedeed
            if key != "pxname" && key != "svname" {
                // force value to 0 if not defined as graphite don't like empty
                // values.
                if len(valueStr) == 0 {
                    valueStr = "0"
                }
                // force string -> int conversion for value
                value, err := strconv.Atoi(valueStr)
                if err != nil {
                    panic(err)
                }
                // DEBUG snippet
                //fmt.Printf("mystore.haproxy.pointblank.%s.%s.%s:%d|g\n",
                //           properties["pxname"],
                //           properties["svname"],
                //           key, value)
                name := properties["pxname"] + 
                        "." + properties["svname"] + 
                        "." + key

                // send to statsd
                cStatsd.Gauge(name, value)
            }
        }
        // this counter is no more used, but can be useful for debugging
        lineCount += 1
    }
    // as we have send all the stats values, close connection with server
    cStatsd.Close()
}

func main() {
    log.Print("Starting haproxy-statsd...") 

    // opts handling
    configFlag := flag.String("config", "config.json", "configuration file location")
    flag.Parse()
    
    // reading config file (mandatory)
    file, errno := os.Open(*configFlag)
    if errno != nil {
        log.Fatal("configuration file is not found or readable")
    }

    // configuration file processing (JSON)
    decoder := json.NewDecoder(file)

    // close file on exit and check for its returned error
    defer func() {
        if err := file.Close(); err != nil {
            panic(err)
        }
    }()

    configuration := Configuration{}
    err := decoder.Decode(&configuration)
    if err != nil {
        panic(err)
    }
    
    // main loop : this will run infinite
    for {
        StatsEmitter(configuration)
        timeDuration := configuration.SleepPeriod * time.Second
        time.Sleep(timeDuration)
    }
}
