package main

import (
	"errors"
	"fmt"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {
	if reflect.ValueOf(out).Kind() != reflect.Ptr {
		return errors.New("return non pointer")
	}

	val := reflect.ValueOf(out).Elem()

	rData := reflect.ValueOf(data)

	switch val.Kind() {
	case reflect.Slice:
		if rData.Type().Kind() != reflect.Slice {
			return fmt.Errorf("expect slice input, but receive %T", data)
		}
		l := rData.Len()

		sliceType := reflect.TypeOf(out).Elem()
		elType := sliceType.Elem()

		val.Set(reflect.MakeSlice(sliceType, l, l))

		for i := 0; i < l; i++ {
			temp := reflect.New(elType)
			if err := i2s(rData.Index(i).Interface(), temp.Interface()); err != nil {
				return fmt.Errorf("failed to convert slice element: %s", err.Error())
			}
			val.Index(i).Set(reflect.Indirect(temp))
		}

	case reflect.Struct:
		if rData.Kind() != reflect.Map {
			return fmt.Errorf("expect map, but receive %v", rData.Kind())
		}
		dataMap, _ := data.(map[string]interface{})
		for i := 0; i < val.NumField(); i++ {
			valueField := val.Field(i)
			typeField := val.Type().Field(i)
			inValue := reflect.ValueOf(dataMap[typeField.Name])
			inValueTypeKind := inValue.Type().Kind()
			switch typeField.Type.Kind() {
			case reflect.Int:
				if inValueTypeKind != reflect.Float64 {
					return fmt.Errorf("expect float64 on field %s, but receive %T", typeField.Name, inValue)
				}
				valueField.SetInt(int64(inValue.Float()))
			case reflect.String:
				if inValueTypeKind != reflect.String {
					return fmt.Errorf("expect string on field %s, but receive %T", typeField.Name, inValue)
				}
				valueField.SetString(inValue.String())
			case reflect.Bool:
				if inValueTypeKind != reflect.Bool {
					return fmt.Errorf("expect bool on field %s, but receive %T", typeField.Name, inValue)
				}
				valueField.SetBool(inValue.Bool())
			case reflect.Struct, reflect.Slice:
				if err := i2s(dataMap[typeField.Name], valueField.Addr().Interface()); err != nil {
					return fmt.Errorf("failed to fill internal field %s: %s", typeField.Name, err.Error())
				}
			}
		}
	}

	return nil
}
