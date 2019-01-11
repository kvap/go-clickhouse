package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type bufReadCloser struct {
	*bytes.Reader
}

func (r *bufReadCloser) Close() error {
	return nil
}

func TestTextRows(t *testing.T) {
	testCases := []*struct {
		name             string
		firstColumnData  string
		secondColumnData string
	}{
		{
			name:             "hello world parsing",
			firstColumnData:  "hello",
			secondColumnData: "world",
		},
		{
			name:             "quoted hello world parsing",
			firstColumnData:  `UserAgent "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots"`,
			secondColumnData: "world",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			input := fmt.Sprintf("Number\tText\nInt32\tString\n1\t%s\n2\t%s\n", tc.firstColumnData, tc.secondColumnData)

			buf := bytes.NewReader([]byte(input))
			rows, err := newTextRows(&conn{}, &bufReadCloser{buf}, time.Local, false)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []string{"Number", "Text"}, rows.Columns())
			assert.Equal(t, []string{"Int32", "String"}, rows.types)
			assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
			assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(1))
			assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
			assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(1))

			dest := make([]driver.Value, 2)
			if !assert.NoError(t, rows.Next(dest)) {
				return
			}

			assert.Equal(t, []driver.Value{int32(1), tc.firstColumnData}, dest)
			if !assert.NoError(t, rows.Next(dest)) {
				return
			}
			assert.Equal(t, []driver.Value{int32(2), tc.secondColumnData}, dest)
			data, err := ioutil.ReadAll(rows.respBody)
			if !assert.NoError(t, err) {
				return
			}

			assert.Equal(t, 0, len(data))
			assert.Equal(t, io.EOF, rows.Next(dest))
			assert.NoError(t, rows.Close())
			assert.Empty(t, data)
		})
	}
}
