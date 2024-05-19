package asg1

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestSumSquares(t *testing.T) {
	inputs := []int{0, 1, -1, 4, -4}
	expected := []int{0, 1, 1, 30, 30}
	for i, v := range inputs {
		if getSumSquares(v) != expected[i] {
			t.Errorf("Mismatch when input = %v", v)
		}
	}
}

func TestGetWords(t *testing.T) {
	inputTxt := "A distributed system is a collection of autonomous computing elements that appears to its users as a single coherent system."
	expectedOutput := []string{"is", "autonomous", "elements", "appears", "its", "users", "as"}
	assertEqual(t, getWords(inputTxt, 's'), expectedOutput)

	inputTxt = "CC552 DS Course"
	expectedOutput = []string{}
	assertEqual(t, getWords(inputTxt, 's'), expectedOutput)

	inputTxt = "Springtime is here!♫ The birds celebrate their return with festive song♫"
	expectedOutput = []string{"here!♫", "song♫"}
	assertEqual(t, getWords(inputTxt, '♫'), expectedOutput)
}

func TestCourseInfo(t *testing.T) {
	records := []RegRecord{
		{1211, "CC551"},
		{1212, "CC552"},
		{1213, "CC553"},
		{1214, "CC551"},
		{1215, "CC551"},
		{1216, "CC552"},
		{1213, "CC553"},
	}
	expectedCourseInfo := make(map[string]int)
	expectedCourseInfo["CC551"] = 3
	expectedCourseInfo["CC552"] = 2
	expectedCourseInfo["CC553"] = 1
	courseInfo := getCourseInfo(records)
	eq := reflect.DeepEqual(courseInfo, expectedCourseInfo)
	if !eq {
		t.Errorf("Mismatch detected. Expected: %v. Obtained: %v", expectedCourseInfo, courseInfo)
	}
}

func TestCount(t *testing.T) {
	list := readInts("list.txt")
	if count(list, 0, 5) != 1 {
		t.Errorf("Mismatch in the count of 0")
	}
	if count(list, 1, 10) != 5 {
		t.Errorf("Mismatch in the count of 1")
	}
	if count(list, 131, 20) != 50 {
		t.Errorf("Mismatch in the count of 131")
	}
}

func readInts(fileName string) []int {
	file, err := os.Open(fileName)
	checkError(err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		checkError(err)
		elems = append(elems, val)
	}
	return elems
}

func equal(list1, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}
	for i := range list1 {
		if list1[i] != list2[i] {
			return false
		}
	}
	return true
}

func assertEqual(t *testing.T, answer, expected []string) {
	if !equal(answer, expected) {
		t.Fatalf(fmt.Sprintf(
			"Words did not match...\nExpected: %v\nActual: %v",
			expected,
			answer))
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

