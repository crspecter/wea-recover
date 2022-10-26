package service

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/schema"

	"strconv"
	"strings"
	"wea-recover/common/def"
	"wea-recover/mysql"
)

func Export(param def.InputInfo) error {
	err := checkExportParam(param)
	if err != nil {
		return err
	}

	// export
	return export(param)
}

func checkExportParam(param def.InputInfo) error {
	return nil
}

func export(param def.InputInfo) error {
	//重建恢复文件
	DeleteFile("./", param.Table+"_recover"+".sql")
	file, ef := CreateAndTruncate("./" + param.Table + "_recover" + ".sql")
	if ef != nil {
		return ef
	}
	file.Close()
	//链接测试库
	conn_src, err := client.Connect(param.Addr+":"+strconv.Itoa(param.Port), param.User, param.Pwd, "test")
	if err != nil {
		println(fmt.Sprintf("connent export source db failed %v", err.Error()))
	}
	table, et := schema.NewTable(conn_src, "test", param.Table+"_recover")
	if et != nil {
		println(fmt.Sprintf("get schema for %s export source db failed %v", param.Table+"_recover", err.Error()))
		return et
	}

	if len(table.PKColumns) != 0 {
		if table.GetPKColumn(0).Type != schema.TYPE_NUMBER {
			return fmt.Errorf("[%s]主键不是数值类型:", param.Table+"_recover")
		}
	}
	//获得主键最大最小值
	minId, me := getMinID(table.Name, table.GetPKColumn(0).Name)
	if me != me {
		return me
	}
	maxId, ae := getMaxID(table.Name, table.GetPKColumn(0).Name)
	if ae != nil {
		return ae
	}

	var fields []string
	var allFiels []string
	for _, field := range table.Columns {
		//是虚拟列,不用拼接到列名中
		if field.IsVirtual {
			allFiels = append(allFiels, fmt.Sprintf("`%s`", field.Name))
			continue
		}
		allFiels = append(allFiels, fmt.Sprintf("`%s`", field.Name))
		fields = append(fields, fmt.Sprintf("`%s`", field.Name))
	}
	fields_sql := strings.Join(fields, ",")
	pSql := fmt.Sprintf(`select %s from %s where %s > ? and %s <=?  order by %s limit %d`,
		fields_sql, table.Name, table.GetPKColumn(0).Name, table.GetPKColumn(0).Name,
		table.GetPKColumn(0).Name, 1000)
	stmt, err := conn_src.Prepare(pSql)

	nowID := minId - 1
	//查询主循环
	for nowID <= maxId {

		ret, eq := stmt.Execute(nowID, maxId)
		if eq != nil {
			fmt.Sprintf("执行[%s]的查询[%s]出现错误[%v]", table.Name, pSql, err.Error())
			return err
		}

		var oneSelect []string
		for row, cv := range ret.Values {
			var lineValue string
			var lineTmp []string
			for i, _ := range cv {
				val, e := ret.GetValue(row, i)
				if e != nil {
					return e
				}
				dataI, isNumber := mysql.ParseNumber(val)
				if isNumber {
					lineTmp = append(lineTmp, fmt.Sprintf("%v", dataI))
				} else {
					lineTmp = append(lineTmp, fmt.Sprintf("'%v'", dataI))
				}
				//value := mysql.ConvertToSqlValueString(v.AsString(), table.Columns[i].RawType)
				if table.Columns[i].Name == table.GetPKColumn(0).Name {
					if isNumber {
						switch pkv := dataI.(type) {
						case int, int8, int16, int32, int64,
							uint, uint8, uint16, uint32, uint64, float32, float64:
							nowID = pkv.(int64)
						}
					} else {
						return fmt.Errorf("主键不是数值类型")
					}
					if err != nil {
						ret.Close()
						return err
					}
				}
			}
			lineValue = fmt.Sprintf("(%v)", strings.Join(lineTmp, ","))
			oneSelect = append(oneSelect, lineValue)
		}
		//写入文件
		sql := toSQL("test."+table.Name+"_recover", fields_sql, oneSelect)
		file.WriteString(sql)
		file.Sync()
		ret.Close()
	}

	return nil
}

func getMaxID(tName string, pk string) (int64, error) {
	sqlStr := fmt.Sprintf("select %s from %s  order by `%s` desc limit 1", pk, "test."+tName, pk)
	rsl := int64(0)
	err := mysql.QueryForRow(sqlStr, &rsl)
	if err != nil {
		fmt.Printf("[%s]执行sql[%s]异常", tName, sqlStr)
		return 0, err
	}
	fmt.Printf("执行sql %s 成功", sqlStr)
	return rsl, err

}

func getMinID(tName string, pk string) (int64, error) {
	sqlStr := fmt.Sprintf("select %s from %s order by `%s`  limit 1", pk, "test."+tName, pk)

	rsl := int64(0)
	err := mysql.QueryForRow(sqlStr, &rsl)
	if err != nil {
		fmt.Printf("[%s]执行sql[%s]异常", tName, sqlStr)
		return 0, err
	}
	return rsl, err

}

func toSQL(table string, field string, value []string) string {
	return fmt.Sprintf("replace into `%s` %s values %s", table, field, strings.Join(value, ",\n"))
}
