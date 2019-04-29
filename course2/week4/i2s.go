package main

import (
	"errors"
	"log"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {
	if reflect.ValueOf(out).Kind() != reflect.Ptr {
		return errors.New("return non pointer")
	}

	val := reflect.ValueOf(out).Elem()

	log.Printf("Input: %#v", data)
	log.Printf("Receive %#v", val)

	switch val.Kind() {
	case reflect.Slice:
		log.Printf("Receive slice")
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			typeField := val.Type().Field(i)
			log.Printf("Process field %#v", typeField)
		}
	}

	return nil
}
