package validator

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"reflect"
	"regexp"
	"strings"
)

var (
	v                     *validator.Validate
	regxSimpleStrNoSpaces = regexp.MustCompile(`^[a-z0-9-]+$`)
)

func init() {
	v = validator.New()

	err := v.RegisterValidation("resource_name", ResourceName)
	if err != nil {
		err = fmt.Errorf("register validator for resource_name error: %w", err)
		panic(err)
	}
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

// ResourceName validate only for lower case alphanumeric and dash character only.
// This not validates required field and empty string is valid.
// You need to add "required" validation to ensure that inputted string is not empty.
func ResourceName(fl validator.FieldLevel) bool {
	field := fl.Field()
	switch field.Kind() {
	case reflect.String:
		if strings.TrimSpace(field.String()) == "" {
			return true
		}

		return regxSimpleStrNoSpaces.MatchString(field.String())
	default:
		return false
	}
}
