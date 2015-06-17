// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path"
)

// This error is returned when the HBASE_HOME environment variable is unset
var ErrHomeUnset = errors.New("Environment variable HBASE_HOME is not set")

func getShellCmd() (*exec.Cmd, *io.WriteCloser, error) {
	hbaseHome := os.Getenv("HBASE_HOME")
	if len(hbaseHome) == 0 {
		return nil, nil, ErrHomeUnset
	}
	hbaseShell := path.Join(hbaseHome, "bin", "hbase")
	cmd := exec.Command(hbaseShell, "shell")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, nil, err
	}
	return cmd, &stdin, nil
}

// CreateTable will find the HBase shell via the HBASE_HOME environment
// variable, and will create the given table with the given families
func CreateTable(table string, cFamilies []string) error {
	cmd, stdin, err := getShellCmd()
	if err != nil {
		return nil
	}

	var buf bytes.Buffer
	buf.WriteString("create '" + table + "'")

	for _, f := range cFamilies {
		buf.WriteString(", '")
		buf.WriteString(f)
		buf.WriteString("'")
	}
	buf.WriteString("\n")

	(*stdin).Write(buf.Bytes())
	(*stdin).Write([]byte("exit\n"))

	err = cmd.Wait()
	return err
}

// DeleteTable will find the HBase shell via the HBASE_HOME environment
// variable, and will disable and drop the given table
func DeleteTable(table string) error {
	cmd, stdin, err := getShellCmd()
	if err != nil {
		return nil
	}

	var buf1 bytes.Buffer
	buf1.WriteString("disable '" + table + "'\n")
	(*stdin).Write(buf1.Bytes())

	var buf2 bytes.Buffer
	buf2.WriteString("drop '" + table + "'\n")
	(*stdin).Write(buf2.Bytes())

	(*stdin).Write([]byte("exit\n"))

	err = cmd.Wait()
	return err
}
