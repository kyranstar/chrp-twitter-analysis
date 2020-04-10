rm(list = ls())
library(neo4r)
library(magrittr)
library(tweetbotornot)
library(rtweet)
library(rjson)
library(tidyverse)
library(dplyr)

## Uses only user-level data, increases number of queries from 
## 180/15 min to 90,000/15 min, but accuracy drops
FAST_PREDICTION = TRUE
LIMIT_PER_IT = 100
NUM_IT = 100

print(
  paste(
    "Running with fast predictions:",
    FAST_PREDICTION,
    "# Iterations:",
    NUM_IT,
    "Predictions per it:",
    LIMIT_PER_IT
  )
)

setwd("C:\\Users\\Owner\\Desktop\\Coding\\chrp\\twitter")

## Read auth tokens for twitter
json_data <- fromJSON(file = "twitter_auth.json")
api_key <- json_data["consumer_key"][['consumer_key']]
api_secret_key <- json_data["consumer_secret"][['consumer_secret']]
access_token <- json_data["access_token"][['access_token']]
access_token_secret <-
  json_data["access_token_secret"][['access_token_secret']]


## authenticate twitter
token <- create_token(
  app = "rstatsjournalismresearch",
  consumer_key = api_key,
  consumer_secret = api_secret_key,
  access_token = access_token,
  access_secret = access_token_secret
)

## Connect to neo4j database
con <- neo4j_api$new(url = "http://localhost:7474",
                     user = "neo4j",
                     password = "password")

for (it in 1:NUM_IT) {
  print(paste("Iteration", it, "/", NUM_IT))
  ## Query unlabeled users
  res <- paste(
    'MATCH (n:User)
  WHERE n.prob_bot IS NULL AND size((n)-[:POSTS]->()) > 4
  RETURN n
  LIMIT',
    LIMIT_PER_IT
  ) %>%
    call_neo4j(con)
  
  df <- bind_cols(purrr::pluck(res, "n"))
  
  users <- df$screen_name
  
  ## get botornot estimates of users
  if (FAST_PREDICTION) {
    results <- tweetbotornot(users, fast = TRUE)
  } else {
    results <- tweetbotornot(users)
  }
  
  df <- df[df$screen_name %in% results$screen_name, ]
  
  df$prob_bot <- results$prob_bot
  
  var_name <- 'prob_bot'
  if (FAST_PREDICTION) {
    var_name <- 'prob_bot_fast'
  }
  
  # Send bot prob back to database
  for (i in 1:nrow(df)) {
    paste(
      'MATCH',
      vec_to_cypher_with_var(df[i, 'screen_name'], "User", a),
      '
    SET a.',
      var_name,
      ' = ',
      df[i, 'prob_bot'],
      '
    RETURN a.screen_name, a.',
      var_name
    ) %>%
      call_neo4j(con)
  }
}
