package validator_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/yusufsyaifudin/khook/pkg/validator"
	"testing"
)

func TestSimplestr(t *testing.T) {
	testCases := []struct {
		Str string `validate:"required"`
		Err bool
	}{
		{
			Str: "",
			Err: true,
		},
		{
			Str: "abc",
			Err: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Str, func(t *testing.T) {
			err := validator.Validate(testCase)
			if !testCase.Err {
				assert.NoError(t, err)
				return
			}

			assert.Error(t, err)
		})
	}
}

func TestSimplestrNoSpace(t *testing.T) {
	testCases := []struct {
		Str string `validate:"resource_name"`
		Err bool
	}{
		{
			Str: "",
			Err: false,
		},
		{
			Str: "abc",
			Err: false,
		},
		{
			Str: "abc-abc",
			Err: false,
		},
		{
			Str: "123-abc",
			Err: false,
		},
		{
			Str: "123-Abc",
			Err: true,
		},
		{
			Str: "123-Abc name-label",
			Err: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Str, func(t *testing.T) {
			err := validator.Validate(testCase)
			if !testCase.Err {
				assert.NoError(t, err)
				return
			}

			assert.Error(t, err)
		})
	}
}
