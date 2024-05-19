package asg1

import (
	// "fmt"
	"strings"
)

// Task 1
// This function should output 0^2 + 1^2 + 2^2 + ... + (|n|)^2
func getSumSquares(n int) int {
	if n < 0 {
		return getSumSquares(n+1) + n*n
	}
	if n > 0 {
		return getSumSquares(n-1) + n*n
	}
	return 0
}

// Task 2
// This function extracts all words ending with endLetter from the string text
// Hints:
// - You may find the strings.Fields method useful.
// - Read about the difference between the types "rune" and "byte", and see how the function is tested.
func getWords(text string, endLetter rune) []string {
	str := strings.Fields(text)
	var words []string
	for _, word := range str {
		runes := []rune(word)
		lastChar := runes[len(runes)-1]
		if lastChar == endLetter {
			words = append(words, word)
		}
	}
	return words
}


// Task 3
type RegRecord struct {
	studentId  int
	courseName string
}

// This method receives a list of student registration records and should return
// a map that shows the number of students registered per course.
// Note that if a duplicate record appears in the input list, it should not be considered in the count.
func getCourseInfo(records []RegRecord) map[string]int {
	courseInfo := make(map[string]int)
	isDuplicate := make(map[string]map[int]bool)
	for _, rec := range records {
		if isDuplicate[rec.courseName] == nil {
			isDuplicate[rec.courseName] = make(map[int]bool)
		}
		if !isDuplicate[rec.courseName][rec.studentId] {
			courseInfo[rec.courseName]++
			isDuplicate[rec.courseName][rec.studentId] = true
		}
	}
	return courseInfo
}

// Task 4
// This method is required to count the occurrences of an input key in a list of integers.
// This should be done in parallel. Each invoked go routine should run the countWorker method on part of the list.
// The communication between the main thread and the workers should be done via channels.
// You can use any way to divide the input list across your workers.
// numThreads will not exceed the length of the array
func count(list []int, key int, numThreads int) int {
	occurrences := 0
	inputChan := make([]chan int, numThreads)
	outChan := make([]chan int, numThreads)

	for i :=0 ; i<numThreads ; i++ {
		inputChan[i] = make(chan int)
		outChan[i] = make(chan int)
	}

	for i, val := range list {
		go countWorker(key, inputChan[i%numThreads], outChan[i%numThreads])
		// fmt.Println("Sending", val, "to worker", i%numThreads)
		inputChan[i%numThreads] <- val
		// fmt.Println("Sent", val, "to worker", i%numThreads)
		occurrences += <-outChan[i%numThreads]
		// fmt.Println("Received", occurrences, "from worker", i%numThreads)
		// time.Sleep(1 * time.Second)
	}

	return occurrences
}

// This worker method receives inputs via inputChan, and outputs the number of occurrences to outChan
// Note: The worker does not have any information about the number of inputs it will process, i.e.,
// the method should keep working as long as inputChan is open
func countWorker(key int, inputChan chan int, outChan chan int) {
	if key == <-inputChan {
		outChan <- 1
	} else {
		outChan <- 0
	}
	// fmt.Println("Worker finished")
}

// func main() {
// 	// fmt.Println(getSumSquares(-1))
// 	// fmt.Println(getWords("A distributed system is a collection of autonomous computing elements that appears to its users as a single coherent system.", 's'))
// 	records := []RegRecord{
// 		{1211, "CC551"},
// 		{1212, "CC552"},
// 		{1213, "CC553"},
// 		{1214, "CC551"},
// 		{1215, "CC551"},
// 		{1216, "CC552"},
// 		{1213, "CC553"},
// 	}
// 	fmt.Println(getCourseInfo(records))
// 	// fmt.Println(count([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 5, 2))
// }
