package main

import (
	// "context"
	"encoding/csv"
	"encoding/json" // For JSON handling
	"fmt"
	"net/http" // For HTTP requests
	"os"       // For OS-related functionality (e.g., environment variables)
	"time"

	twitterscraper "github.com/imperatrona/twitter-scraper"
)

// Custom cookie struct to handle the "expires" field
type CookieJSON struct {
	Name     string  `json:"name"`
	Value    string  `json:"value"`
	Domain   string  `json:"domain"`
	Path     string  `json:"path"`
	Expires  float64 `json:"expires"` // Expires as a Unix timestamp (float64)
	HttpOnly bool    `json:"httpOnly"`
	Secure   bool    `json:"secure"`
	SameSite string  `json:"sameSite,omitempty"`
}

// Convert "SameSite" and "Expires" fields to appropriate Go types
func convertSameSite(value string) http.SameSite {
	switch value {
	case "Lax":
		return http.SameSiteLaxMode
	case "Strict":
		return http.SameSiteStrictMode
	case "None":
		return http.SameSiteNoneMode
	default:
		return http.SameSiteDefaultMode
	}
}

// func main() {
//     scraper := twitterscraper.New()

// 	// Open the cookies file
// 	f, err := os.Open("cookies.json")
// 	if err != nil {
// 		panic("Failed to open cookies file: " + err.Error())
// 	}
// 	defer f.Close()

// 	// Deserialize JSON into custom CookieJSON struct
// 	var cookiesJSON []CookieJSON
// 	err = json.NewDecoder(f).Decode(&cookiesJSON)
// 	if err != nil {
// 		panic("Error decoding cookies: " + err.Error())
// 	}

// 	// Convert custom cookies to http.Cookie
// 	var cookies []*http.Cookie
// 	for _, c := range cookiesJSON {
// 		var expires time.Time
// 		if c.Expires > 0 {
// 			// Convert Unix timestamp to time.Time
// 			expires = time.Unix(int64(c.Expires), 0)
// 		}

// 	cookies = append(cookies, &http.Cookie{
// 		Name:     c.Name,
// 		Value:    c.Value,
// 		Domain:   c.Domain,
// 		Path:     c.Path,
// 		Expires:  expires,
// 		Secure:   c.Secure,
// 		HttpOnly: c.HttpOnly,
// 		SameSite: convertSameSite(c.SameSite),
// 	})
// 	}

//     // Create a CSV file to save tweets
//     csvFile, err := os.Create("nicltimiraos_test.csv")
//     if err != nil {
//         fmt.Println("Error creating CSV file:", err)
//         return
//     }
//     defer csvFile.Close()

//     // Initialize CSV writer
//     writer := csv.NewWriter(csvFile)
//     defer writer.Flush()

//     // Write the header row
//     writer.Write([]string{"TimeParsed", "Tweet Text", "Likes", "Retweets", "Views", "Username", "IsRetweet", "IsReply", "TweetID"})

//     // Fetch tweets and save them
//     for tweet := range scraper.GetTweets(context.Background(), "nicktimiraos", 3000) {
//         if tweet.Error != nil {
//             fmt.Println("Error fetching tweet:", tweet.Error)
//             continue
//         }
//         fmt.Println(tweet.TimeParsed)

//         // Write tweet text to the CSV file
// 		err = writer.Write([]string{
// 			tweet.TimeParsed.Format(time.RFC3339),
// 			tweet.Text,
// 			fmt.Sprintf("%d", tweet.Likes),
// 			fmt.Sprintf("%d", tweet.Retweets),
// 			fmt.Sprintf("%d", tweet.Views),
// 			tweet.Username,
// 			fmt.Sprintf("%t",tweet.IsRetweet),
// 			fmt.Sprintf("%t",tweet.IsReply),
// 			tweet.ID,
// 		})
//         if err != nil {
//             fmt.Println("Error writing to CSV:", err)
//         }
//     }

//     fmt.Println("Tweets saved to tweets.csv")
// }

//  2fa: ub9p8nq2rp3u

func main() {
	scraper := twitterscraper.New()

	// Open the cookies file
	f, err := os.Open("session.tw_session")
	if err != nil {
		panic("Failed to open cookies file: " + err.Error())
	}
	defer f.Close()

	var cookiesJSON []CookieJSON
	err = json.NewDecoder(f).Decode(&cookiesJSON)
	if err != nil {
		panic("Error decoding cookies: " + err.Error())
	}

	var cookies []*http.Cookie
	for _, c := range cookiesJSON {
		var expires time.Time
		if c.Expires > 0 {
			expires = time.Unix(int64(c.Expires), 0)
		}

		cookies = append(cookies, &http.Cookie{
			Name:     c.Name,
			Value:    c.Value,
			Domain:   c.Domain,
			Path:     c.Path,
			Expires:  expires,
			Secure:   c.Secure,
			HttpOnly: c.HttpOnly,
			SameSite: convertSameSite(c.SameSite),
		})
	}

	err = scraper.Login("JCZheng23440", "Zheng_18Jeff")
	if err != nil {
		fmt.Println("Error logging in:", err)
		return
	}

	csvFileName := "nicktimiraos_test2.csv"
	csvFile, err := os.Create(csvFileName)
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write header row
	writer.Write([]string{"TimeParsed", "Tweet Text", "Likes", "Retweets", "Views", "Username", "IsRetweet", "IsReply", "TweetID"})

	var cursor string
	requestsMade := 0
	totalTweetsFetched := 0

	for {
		// Fetch tweets
		tweets, nextCursor, err := scraper.FetchTweets("nicktimiraos", 20, cursor)
		if err != nil {
			fmt.Println("Error fetching tweets:", err)
			break
		}

		for _, tweet := range tweets {
			err := writer.Write([]string{
				tweet.TimeParsed.Format(time.RFC3339),
				tweet.Text,
				fmt.Sprintf("%d", tweet.Likes),
				fmt.Sprintf("%d", tweet.Retweets),
				fmt.Sprintf("%d", tweet.Views),
				tweet.Username,
				fmt.Sprintf("%t", tweet.IsRetweet),
				fmt.Sprintf("%t", tweet.IsReply),
				tweet.ID,
			})
			if err != nil {
				fmt.Println("Error writing to CSV:", err)
			}
			totalTweetsFetched++
		}

		writer.Flush() // Flush writes to ensure data is saved
		cursor = nextCursor
		requestsMade++

		// Check if rate limit is reached
		if requestsMade >= 140 {
			fmt.Printf("Reached rate limit after 140 requests. Fetched %d tweets so far.\n", totalTweetsFetched)
			fmt.Println("Sleeping for 15 minutes...")
			time.Sleep(15 * time.Minute)
			requestsMade = 0 // Reset request counter
		}

		// Break if there are no more tweets to fetch
		if nextCursor == "" {
			fmt.Println("No more tweets to fetch. Exiting...")
			break
		}
	}

	fmt.Printf("Finished fetching tweets. Total tweets saved: %d\n", totalTweetsFetched)
}
