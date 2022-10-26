package mysql

import (
	"fmt"

	"github.com/siddontang/go/hack"
	"reflect"
	"strings"
	"unsafe"

	"github.com/gogf/gf/encoding/gbinary"
	"github.com/gogf/gf/text/gregex"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
)

var (
	DONTESCAPE = byte(255)

	EncodeMap [256]byte
)

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range EncodeMap {
		EncodeMap[i] = DONTESCAPE
	}
	for i := range EncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			EncodeMap[byte(i)] = to
		}
	}
}

func ConvertToSqlValueString(fieldValue []byte, fieldType string) string {
	data := ConvertValue(fieldValue, fieldType)
	dataI, isNumber := parseNumber(data)
	if isNumber {
		return fmt.Sprintf("%v", dataI)
	}
	return fmt.Sprintf("'%v'", dataI)
}

// mysql 的byte转类型
func ConvertValue(fieldValue []byte, fieldType string) interface{} {
	if fieldValue == nil {
		return nil
	}
	t, _ := gregex.ReplaceString(`\(.+\)`, "", fieldType)
	t = strings.ToLower(t)
	switch t {
	case
		"binary",
		"varbinary",
		"blob",
		"tinyblob",
		"mediumblob",
		"longblob":
		return fieldValue

	case
		"int",
		"tinyint",
		"small_int",
		"smallint",
		"medium_int",
		"mediumint",
		"serial":
		if gstr.ContainsI(fieldType, "unsigned") {
			gconv.Uint(string(fieldValue))
		}
		return gconv.Int(string(fieldValue))

	case
		"big_int",
		"bigint",
		"bigserial":
		if gstr.ContainsI(fieldType, "unsigned") {
			gconv.Uint64(string(fieldValue))
		}
		return gconv.Int64(string(fieldValue))

	case "real":
		return gconv.Float32(string(fieldValue))

	case
		"float",
		"double",
		"decimal",
		"money",
		"numeric",
		"smallmoney":
		return gconv.Float64(string(fieldValue))

	case "bit":
		s := string(fieldValue)
		// mssql is true|false string.
		if strings.EqualFold(s, "true") {
			return 1
		}
		if strings.EqualFold(s, "false") {
			return 0
		}
		return gbinary.BeDecodeToInt64(fieldValue)

	case "bool":
		return gconv.Bool(fieldValue)
	case
		"date",
		"datetime",
		"timestamp":
		return string(fieldValue)
	default:
		// Auto detect field type, using key match.
		switch {
		case strings.Contains(t, "text") || strings.Contains(t, "char") || strings.Contains(t, "character"):
			return string(fieldValue)

		case strings.Contains(t, "float") || strings.Contains(t, "double") || strings.Contains(t, "numeric"):
			return gconv.Float64(string(fieldValue))

		case strings.Contains(t, "bool"):
			return gconv.Bool(string(fieldValue))

		case strings.Contains(t, "binary") || strings.Contains(t, "blob"):
			return fieldValue

		case strings.Contains(t, "int"):
			return gconv.Int(string(fieldValue))

		case strings.Contains(t, "time"):
			return string(fieldValue)

		case strings.Contains(t, "date"):
			return string(fieldValue)
		default:
			return string(fieldValue)
		}
	}
}

func parseNumber(d interface{}) (interface{}, bool) {
	switch v := d.(type) {
	case string:
		return Escape(v), false
	case []byte:
		return Escape(HackString(v)), false
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return v, true
	case nil:
		// nil 相当于数字
		return "null", true
	default:
		// 其他??
		return v, true
	}
}

func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))

	for _, w := range hack.Slice(sql) {
		if c := EncodeMap[w]; c == DONTESCAPE {
			dest = append(dest, w)
		} else {
			dest = append(dest, '\\', c)
		}
	}

	return string(dest)
}

func HackString(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}
