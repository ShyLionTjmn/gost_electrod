package main

import (
  "time"
  "regexp"
  "math"
)

var time_regex *regexp.Regexp
var date_regex *regexp.Regexp
var num_regex *regexp.Regexp

const TIME_DIFF_WARN float64 = 30 // minutes

func init() {
  time_regex = regexp.MustCompile("^TIME_\\(([0-9]{2}):([0-9]{2}):([0-9]{2})\\)\r\n")
  date_regex = regexp.MustCompile("^DATE_\\([0-9]{2}\\.([0-9]{2})\\.([0-9]{2})\\.([0-9]{2})\\)\r\n")
  num_regex = regexp.MustCompile("^[0-9]+(?:\\.[0-9]+)?")
}

func processData(data t_scanData, ws t_workStruct) (error) {
  now := time.Now().In(ws.location)

  period := now.Format("20060102")
  ts := now.Unix()

  if opt_d { logMessage("Period:", period); }
  if opt_d { logMessage("ts:", ts); }

  warn_str := ""

  time_val, t_ok := data.data["TIME_()"]
  date_val, d_ok := data.data["DATE_()"]

  if t_ok && d_ok {
    time_parts := time_regex.FindStringSubmatch(time_val)
    date_parts := date_regex.FindStringSubmatch(date_val)
    if time_parts != nil && date_parts != nil && len(time_parts) == 4 && len(date_parts) == 4 {
      counter_time, parse_err := time.ParseInLocation("20060102150405", "20"+date_parts[3]+date_parts[2]+date_parts[1]+time_parts[1]+time_parts[2]+time_parts[3], ws.location)
      if parse_err == nil {
        if opt_d { logMessage("Counter time is:", counter_time); }
        time_diff_abs := math.Abs(now.Sub(counter_time).Round(time.Minute).Minutes())
        if opt_d { logMessage("Time difference is:", time_diff_abs); }

        if time_diff_abs > TIME_DIFF_WARN {
          warn_str += "Device time is wrong: "+counter_time.Format("2006-01-02 15:04:05 MST")+"\n"
        }
      } else {
        if opt_d { logError("time parsing error:", parse_err.Error()); }
      }
    }
  }


  for key, value := range data.data {
    //key_len=len(key)
    val_len := len(value)

    if val_len >= 2 && value[val_len-2] == '\r' && value[val_len-1] == '\n' {
      val_len -= 2
      value = value[:val_len]
    }

    if val_len >= 7 && value[5] == '(' && value[val_len-1] == ')' {
      value = value[6:val_len-1]
      val_len -= 7
    }

    if opt_d { logMessage(key, " = ", value) }

    if !opt_r {
      // save to database
      _, db_err := db.Exec("INSERT INTO ds(d_name, d_value, d_time, d_fk_c_id) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE d_value=VALUES(d_value), d_time=VALUES(d_time)",
                           key, value, ts, data.c_id)
      if db_err != nil {
        if opt_d { logError(db_err.Error()); }
        return db_err
      }
    }

    if val_len >= 3 && value[:3] == "ERR" {
      warn_str += key + " = " + value + "\n"
    } else if !opt_r {
      if (key == "ET0PE(1)" || key == "ET0PE(2)" || key == "ET0PE(3)") && num_regex.MatchString(value) {
        _, db_err := db.Exec("INSERT INTO rs SET r_name=?, r_value=?, r_time=?, r_fk_c_id=?, r_date=? ON DUPLICATE KEY UPDATE r_value=VALUES(r_value), r_time=VALUES(r_time)",
                             key, value, ts, data.c_id, period)
        if db_err != nil {
          if opt_d { logError(db_err.Error()); }
          return db_err
        }
      }

      if (key == "ET0PE(1)" || key == "ET0PE(2)" || key == "ET0PE(3)" ||
          key == "VOLTA(1)" || key == "VOLTA(2)" || key == "VOLTA(3)" ||
          key == "CURRE(1)" || key == "CURRE(2)" || key == "CURRE(3)") &&
         num_regex.MatchString(value) {
        logMessage("Graph key ", key, ":", value)
      }
    }
  }

  if opt_d { logMessage(warn_str) }

  if !opt_r {
    _, db_err := db.Exec("INSERT INTO ds(d_name, d_value, d_time, d_fk_c_id) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE d_value=VALUES(d_value), d_time=VALUES(d_time)",
                           "_warn_", warn_str, ts, data.c_id)
    if db_err != nil {
      if opt_d { logError(db_err.Error()); }
      return db_err
    }
  }

  return nil
}
