package main

import (
  "fmt"
  "sync"
  "os"
  "syscall"
  "os/signal"
  "time"
  "flag"
  "regexp"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
)

const DSN= "counterd:counterd@unix(/var/lib/mysql/mysql.sock)/counters"
const DB_REFRESH_TIME= 10
const DB_ERROR_TIME= 60

const DATA_BUFFER_LEN= 100

type t_scanType byte

const (
  r_data        t_scanType = iota
  r_serial      t_scanType = iota //in str
  r_error       t_scanType = iota //in str
  r_debug       t_scanType = iota //in str
)


var opt_d bool= false
var opt_D bool= false
var opt_r bool= false
var opt_i string

var db *sql.DB=nil
var db_ok bool=false
var workers map[int]t_workStruct

type t_timePercents struct {
  perc_connect float64
  perc_work float64
  perc_error float64
}

type t_scanData struct {
  ret_type      t_scanType
  c_id          int
  data          map[string]string
  str           string
  added         time.Time
  time_percents t_timePercents
  traff_in      uint64
  traff_out     uint64
}

const (
  c_stop        = "stop"
)

type t_workStruct struct {
  c_id          int
  c_connect     string
  c_ip          string
  c_port        string
  c_address     string
  c_serial      string
  c_type        string
  c_tz          string
  wg            *sync.WaitGroup
  control_ch    chan string
  data_ch       chan t_scanData
  check         time.Time
  added         time.Time
  location      *time.Location
}

var snumb_regex *regexp.Regexp

func main() {

  var f_opt_d *bool = flag.Bool("d", opt_d, "Debug output")
  var f_opt_D *bool = flag.Bool("D", opt_D, "Debug packet output")
  var f_opt_r *bool = flag.Bool("r", opt_r, "Read only mode (do not update mysql tables)")
  var f_opt_i *string = flag.String("i", opt_i, "Work only this IP")

  ip_regex := regexp.MustCompile("^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$")
  connect_regex := regexp.MustCompile("^([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}):([0-9]{1,5})(?:/([0-9a-zA-Z]+))?$")
  snumb_regex = regexp.MustCompile("^SNUMB\\(([0-9a-zA-Z]+)\\)\r\n$")

  flag.Parse()

  opt_d = *f_opt_d
  opt_D = *f_opt_D
  opt_r = *f_opt_r
  opt_i = *f_opt_i

  sig_ch := make(chan os.Signal, 1)

  signal.Notify(sig_ch, syscall.SIGHUP)
  signal.Notify(sig_ch, syscall.SIGINT)
  signal.Notify(sig_ch, syscall.SIGTERM)
  signal.Notify(sig_ch, syscall.SIGQUIT)

  data_ch := make(chan t_scanData, DATA_BUFFER_LEN)

  workers = make(map[int]t_workStruct)

  var wg sync.WaitGroup

  setStatus("Started")

MAIN_LOOP: for { //main loop
    cycle_start := time.Now()
    //if(opt_d) { logMessage("main", "Cycle start") }

    if(!db_ok && db != nil) {
      db.Close()
      db=nil
    }
    //open db if not open
    if(db == nil) {
      var err error
      db, err = sql.Open("mysql", DSN)
      if( err != nil ) {
        logError("main", err.Error())
        setStatus("DB Open error: "+err.Error())
        db_ok=false
        db=nil
      } else {
        db_ok=true
        if(opt_d) { logMessage("main", "DB Opened") }
      }
    }
    if(db != nil) {
      //check if db is alive
      err := db.Ping()
      if( err != nil ) {
        logError("main", err.Error())
        setStatus("DB Ping error: "+err.Error())
        db_ok=false
        db.Close()
        db = nil
      } else {
        //if(opt_d) { logMessage("main", "DB Ok") }
      }
    }

    if(db_ok) {
      //query cs table to check for new/paused cs
      query := "SELECT c_id, c_connect, c_serial, c_type, c_tz FROM cs WHERE c_paused = 0 AND c_deleted = 0 AND c_type = 'gost-c-electro-1p'"
      if( opt_i != "" && ip_regex.MatchString(opt_i)) {
        query += " AND c_connect LIKE '"+opt_i+":%'"
      }

      rows, err := db.Query(query)
      if( err != nil) {
        logError("main", err.Error())
        setStatus("DB SELECT error: "+err.Error())
        db_ok=false
      } else {
        //fetch rows from cs table
        for( rows.Next() && db_ok) {
          var db_c_id int
          var db_c_connect string
          var db_c_serial string
          var db_c_type string
          var db_c_tz string
          err := rows.Scan(&db_c_id, &db_c_connect, &db_c_serial, &db_c_type, &db_c_tz)
          if( err != nil ) {
            logError("main", err.Error())
            setStatus("DB Scan error: "+err.Error())
            db_ok=false
          } else {
            //check if c worker present
            _, ok := workers[db_c_id]
            if(ok &&
               db_c_connect == workers[db_c_id].c_connect &&
               db_c_serial == workers[db_c_id].c_serial &&
               db_c_type == workers[db_c_id].c_type &&
               db_c_tz == workers[db_c_id].c_tz &&
            true) {
              //refresh worker data
              ws := workers[db_c_id]
              ws.check=cycle_start
              workers[db_c_id]=ws
            } else {
              if(ok) {
                //ok, but restart required
                if(opt_d) { logMessage("main", "c:", db_c_id,"ip:",db_c_connect,", DB data changed, restart") }
                workers[db_c_id].control_ch <- c_stop
                close(workers[db_c_id].control_ch)
                delete(workers, db_c_id)
              } else {
                if(opt_d) { logMessage("main", "c",db_c_id," appeared, ip:",db_c_connect,", start") }
              };

              matches := connect_regex.FindStringSubmatch(db_c_connect)
              if matches != nil {
                time_location, time_err := time.LoadLocation(db_c_tz)
                if time_err != nil {
                  ts := time.Now().Unix()
                  db.Exec("UPDATE cs SET c_error=?, c_last_error=? WHERE c_id=?", "Time zone load error: "+time_err.Error(), ts, db_c_id)
                } else {
                  workers[db_c_id]=t_workStruct{
                    c_id:            db_c_id,
                    c_connect:       db_c_connect,
                    c_ip:            matches[1],
                    c_port:          matches[2],
                    c_address:       matches[3],
                    c_serial:        db_c_serial,
                    c_type:          db_c_type,
                    c_tz:            db_c_tz,
                    wg:              &wg,
                    control_ch:      make(chan string, 1),
                    data_ch:         data_ch,
                    check:           cycle_start,
                    added:           time.Now(),
                    location:        time_location,
                  }

                  wg.Add(1)
                  go worker(workers[db_c_id])
                }
              } else {
                logError("main", "Bad connect string for: ",db_c_id," :",db_c_connect)
                ts := time.Now().Unix()
                db.Exec("UPDATE cs SET c_error=?, c_last_error=? WHERE c_id=?", "Bad connect string", ts, db_c_id)
              }
            }
          }
        }
        if(db_ok) {
          rows.Close()
          for c_id, _ := range workers {
            if(workers[c_id].check != cycle_start) {
              if(opt_d) { logMessage("main", "c",c_id,", ip:",workers[c_id].c_connect,", paused or gone, stop") }
              //c gone or paused, signal stop
              workers[c_id].control_ch <- c_stop
              close(workers[c_id].control_ch)
              delete(workers, c_id)
            }
          }
        }
      } //cs refresh
    }

    var db_timer *time.Timer
    if(db_ok) {
      //no error on db, wait DB_REFRESH_TIME
      db_timer=time.NewTimer(DB_REFRESH_TIME*time.Second)
    } else {
      db_timer=time.NewTimer(DB_ERROR_TIME*time.Second)
    }

    //wait for something

    var data t_scanData

    for {
      select {
        case s := <-sig_ch:
          db_timer.Stop()
          if(s == syscall.SIGHUP || s == syscall.SIGUSR1) {
            if(opt_d) { logMessage("Got data refresh signal") }
            continue MAIN_LOOP
          }
          break MAIN_LOOP
        case <-db_timer.C:
          continue MAIN_LOOP
        case data = <-data_ch:
      }
      //we've got data

      ts := time.Now().Unix()

      wd,we := workers[data.c_id]
      if(we && wd.added==data.added) {
        if(!opt_r) {
          var err error

          if(data.ret_type == r_data) {
            db_err := processData(data, wd)
            if( db_err != nil) {
              db_ok=false
            } else {
              _, err =db.Exec("UPDATE cs SET c_last_ok=? WHERE c_id=?", ts, data.c_id)
              if( err != nil) {
                logError("main", err.Error())
                setStatus("DB UPDATE error: "+err.Error())
                db_ok=false
              }
            }
          } else if(data.ret_type == r_serial) {
            fmt.Println("Got serial from",data.c_id,data.str)

            if(workers[data.c_id].c_serial != data.str) {
              if(workers[data.c_id].c_serial == "auto") {
                _, err =db.Exec("UPDATE cs SET ts=?, c_serial=?, change_by = 'daemon'  WHERE c_id=?", ts, data.str, data.c_id)
                if( err != nil) {
                  logError("main", err.Error())
                  setStatus("DB UPDATE error: "+err.Error())
                  db_ok=false
                } else {
                  // worker will be restarted later
                }
              } else {
                //some weird stuff happened
                //do nothing, worker will send error, no point to restart it, as serial wont fix himself
              }
            }
          } else if(data.ret_type == r_error) {
            // worker had problem, report it
            _, err =db.Exec("UPDATE cs SET c_error=?, c_last_error=? WHERE c_id=?", data.str, ts, data.c_id)
            if( err != nil) {
              logError("main", err.Error())
              setStatus("DB UPDATE error: "+err.Error())
              db_ok=false
            }
          } else {
            fmt.Println("Got something from %d: %v\n", data.c_id, data)
          }
        } else {
          if(data.ret_type == r_data) {
            if(opt_d) { fmt.Printf("Got Data from %d: %v\n", data.c_id ,data.data) }
          } else if(data.ret_type == r_serial) {
            if(opt_d) { fmt.Printf("Got serial from %d: %s\n", data.c_id, data.str) }
          } else if(data.ret_type == r_error) {
            if(opt_d) { fmt.Printf("Got error from %d: %s\n", data.c_id, data.str) }
          } else {
            if(opt_d) { fmt.Println("Got something from %d: %v\n", data.c_id, data) }
          }
        }
      } else {
        //data from dead
        if(opt_d) { fmt.Fprintln(os.Stderr, "Got data from DEAD goroutine",data.c_id,data.ret_type,data.str) }
      }
    } //select loop
  } //main loop
  if(opt_d) { logMessage("Time to stop") }
  for c_id, _ := range workers {
    workers[c_id].control_ch <- c_stop
    close(workers[c_id].control_ch)
  }
  wg.Wait()
  if(db != nil) {
    db.Close()
    db=nil;
  }
  if(opt_d) { logMessage("Done, bye-bye") }
}
