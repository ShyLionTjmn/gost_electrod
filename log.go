package main

import "fmt"
import "os"

func logError(message... interface{}) {
  fmt.Fprintln(os.Stderr, message...)
}

func logMessage(message... interface{}) {
  fmt.Println(message...)
}

func setStatus(status string) {
  println("Set status to", status)
  //do nothing yet
}

