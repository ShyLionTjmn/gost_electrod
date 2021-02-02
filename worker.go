package main

import (
  _ "fmt"
  "time"
  "github.com/ShyLionTjmn/GOST-61107-TCP"
)

const SCAN_INTERVAL = 15*time.Second

func worker(ws t_workStruct) {
  if(opt_d) { logMessage("worker", ws.c_id,"ip:", ws.c_connect, "started") }
  defer ws.wg.Done()

  var err error
  dev := GOST_61107_TCP.Init(ws.c_ip, ws.c_port, ws.c_address, "1", time.Second, ws.control_ch)
  dev.Debug=opt_D

  defer func() { if dev.Connected { dev.Close() } }()

  var err_str string
  result_map := make(map[string]string)
  commands := []string{"TIME_()", "DATE_()", "MODEL()", "IDPAS()", "ET0PE(1)", "ET0PE(2)", "ET0PE(3)", "VOLTA(1)", "CURRE(1)"}
  if ws.c_type == "gost-c-electro-3p" {
    commands = append(commands, "VOLTA(2)", "CURRE(2)", "VOLTA(3)", "CURRE(3)")
  }

  var qres *GOST_61107_TCP.Message

  var cycle_start time.Time
  var elapsed time.Duration

  var timer *time.Timer

MAIN_CYCLE:
  for {

    cycle_start = time.Now()

    err_str = ""

    err = dev.Connect()
    if err != nil {
      if err.Error() == "exit signalled" { break MAIN_CYCLE }
      err_str = "Connect error: "+err.Error()
    }

    if err_str == "" {
      _, err = dev.ReadMessage()
      if err != nil {
        if err.Error() == "exit signalled" { break MAIN_CYCLE }
        err_str = "Connect error: "+err.Error()
      }
    }

    // read serial
    if err_str == "" {
      qres, err = dev.Query("R1", "SNUMB()")
      if err != nil {
        if err.Error() == "exit signalled" { break MAIN_CYCLE }
        err_str = "Query error: "+err.Error()
      }
    }

    // check serial in qres.Body

    //

    // fetch data

    for _, cmd := range commands {
      if err_str == "" {
        qres, err = dev.Query("R1", cmd)
        if err != nil {
          if err.Error() == "exit signalled" { break MAIN_CYCLE }
          err_str = "Query error: "+err.Error()
        }
        result_map[cmd] = qres.Body
      }
    }

    if dev.Connected {
      dev.Close()
    }

    if err_str == "" {
      ws.data_ch <- t_scanData{c_id: ws.c_id, data: result_map, ret_type: r_data, added: ws.added}
    } else {
      ws.data_ch <- t_scanData{c_id: ws.c_id, str: err_str, ret_type: r_error, added: ws.added}
    }

    elapsed = time.Now().Sub( cycle_start )

    timer = time.NewTimer( SCAN_INTERVAL - elapsed )
    time.Sleep(time.Millisecond)

    select {
      case cmd, ok := <-ws.control_ch:
        if !timer.Stop() { <-timer.C } // stop timer and drain channel
        if(!ok) {
          //control channel closed, quit
          if(opt_d) { logMessage("worker", ws.c_id,"ip:", ws.c_connect, "quit because control channel closed") }
          return
        }
        if(cmd == c_stop) {
          if(opt_d) { logMessage("worker", ws.c_id,"ip:", ws.c_connect, "quit because control Stop received") }
          //got stop command, quit
          return
        }
        //unknown command, quit
        if(opt_d) { logMessage("worker", ws.c_id,"ip:", ws.c_connect, "quit because unknown control received") }
        return
      case <-timer.C:
        //no quit command, go-on
    }
  }
}
