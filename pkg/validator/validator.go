package validator

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"reflect"
)

var (
	v *validator.Validate
)

func init() {
	v = validator.New()

}

type CustomValidate interface{ Validate() error }

func validateCustom(i interface{}) error {
	rv := reflect.ValueOf(i)

	// dereference pointer
	for rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)

		for field.Kind() == reflect.Ptr {
			field = field.Elem()
		}

		if field.Kind() != reflect.Struct {
			continue
		}

		val := field.Interface()
		if cv, ok := (val).(CustomValidate); ok && cv != nil {
			err := cv.Validate()
			if err != nil {
				return err
			}

			return validateCustom(field.Interface())
		}
	}

	return nil
}

func Validate(i interface{}) error {
	if i == nil {
		return fmt.Errorf("data to validate is nil")
	}

	err := validateCustom(i)
	if err != nil {
		return err
	}

	return v.Struct(i)
}
