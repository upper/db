package yaml

import "testing"

func TestRead(t *testing.T) {
	Open("examples/input/settings.yaml")
}

func TestGet(t *testing.T) {
	settings := Open("examples/input/settings.yaml")

	test1 := "Hello World!"
	val1 := settings.Get("test_string", nil).(string)

	if val1 != test1 {
		t.Errorf("Got %t expecting %t.", val1, test1)
	}

	test2 := -23
	val2 := settings.Get("non_defined_int", test2).(int)

	if val2 != test2 {
		t.Errorf("Got %t expecting %t.", val1, test1)
	}

	test3 := "Third"
	val3 := settings.Get("test_map/element_3/test_sequence", nil).([]interface{})

	if val3[2] != test3 {
		t.Errorf("Got %t expecting %t.", val3[2], test3)
	}

	test4 := "HaS CaSe"
	val4 := settings.Get("test_caseless", nil).(string)

	if val4 != test4 {
		t.Errorf("Got %t expecting %t.", val4, test4)
	}

	test5 := "Hello World!"
	val5 := settings.GetString("test_string")

	if test5 != val5 {
		t.Errorf("Got %t expecting %t.", test5, val5)
	}

	test6 := 1234
	val6 := settings.GetInt("test_int")

	if test6 != val6 {
		t.Errorf("Got %t expecting %t.", test6, val6)
	}

	test7 := 1.2
	val7 := settings.GetFloat("test_float")

	if test7 != val7 {
		t.Errorf("Got %t expecting %t.", test7, val7)
	}

	test8 := true
	val8 := settings.GetBool("test_bool")

	if test8 != val8 {
		t.Errorf("Got %t expecting %t.", test8, val8)
	}

}

func TestSet(t *testing.T) {
	settings := Open("examples/input/settings.yaml")

	settings.Set("test_map/element_3/test_bool", true)

	test1 := true
	val1 := settings.Get("test_map/element_3/test_bool", nil).(bool)

	if val1 != test1 {
		t.Errorf("Got %t expecting %t.", val1, test1)
	}

}

func TestWrite(t *testing.T) {
	settings := New()
	defer settings.Write("examples/input/settings2.yaml")

	settings.Set("test_map/element_3/test_bool", true)

}
