# Data Deliverable Report - PiazzaGate

**Team members**: Nada Benabla, John Chung, Alex Ding, and Megan Frisella

## Step 1

1. We obtain a dataset of COVID-related tweet IDs annotated with geotags from [this dataset](https://twitterdata.covid19dataresources.org/), filtering for US-based tweets with city-level annotation, and we rehydrate the tweets using twarc. We obtain a dataset mapping cities to counties from [data.world](https://data.world/niccolley/us-zipcode-to-county-state). We obtain a dataset of COVID cases by date/county from [The New York Times](https://github.com/nytimes/covid-19-data). We collect vaccination data by date/county from the Center for Disease Control and Prevention (CDC) using their API; we process the API responses using sodapy and use Pandas to convert the results into a dataframe. We downloaded three datasets from the United States Census Bureau: demographic and housing estimates, economic characteristics, and social characteristics for each county in the country. We also downloaded a dataset from MIT that contains county-level returns for presidential elections from 2000 to 2020. We loaded the columns of interest from each dataset into a dictionary, then created a SQL database from it.

2. Our COVID cases, vaccination data, and demographic information come from well-established organizations. The city/county dataset is created by a third party using data from reputable sources such as US HUD, Census Bureau, and USPS. In choosing a geotagged COVID tweets dataset, we are primarily interested in a wide timespan and high quantity, since we need to further process them to select for misinformation tweets. Therefore, instead of only using tweets with GPS coordinates attached (which is a tiny portion), we opt for a dataset that automatically infers geolocation based on user profile, tweet geotags, and the tweet itself. The dataset we choose is created by researchers at reputable universities.

## Step 2

- Tweets
  - id INT primary key
  - fips INT foreign key references demographics(fips): federal information processing standards (FIPS) ID of tweet’s annotated location
  - city TEXT
  - user_id INT foreign key references users(id)
  - text TEXT: text of the tweet or, if this is a retweet, text of the original tweet
  - created_at TEXT
  - is_retweet INT
  - original_tweet_id OPTIONAL INT
  - retweet_count INT 
  - favorite_count INT
  - lang TEXT: language of the tweet 
Users
  - id INT primary key
  - location TEXT: user-entered location
  - verified INT: whether the account is verified
  - follower_cnt INT
  - list_cnt INT
  - tweet_cnt INT
Hashtags
  - tweet_id INT foreign key references tweets(id)
  - hashtag TEXT: the text of the hashtag
Covid: COVID cases and vaccination by county per day
  - date TEXT
  - county TEXT
  - fips INT foreign key references demographics(fips)
  - cases INT
  - deaths INT
  - state TEXT
  - vaccinated_count INT : total number of people who are fully vaccinated in county
  - vaccinated_percent REAL : percentage of people who are fully vaccinated in county
  - PRIMARY KEY (date, fips)
Demographics: demographics and voting information by county
  - fips INT primary key
  - county TEXT
  - state TEXT
  - total_population INT
  - percent_age REAL (Have columns for ages 10 - 85+)
  - percent_race REAL (Have columns for various race groups)
  - unemployment_rate REAL
  - mean_household_income INT
  - percent_health_insurance REAL (Have columns for different types of health insurance)
  - percent_income_below_poverty_line REAL
  - percent_household REAL (Have columns for different types of households)
  - percent_male_marriage_status REAL (Have columns for different types of statuses)
  - percent_female_marriage_status REAL (Have columns for different types of statuses)
  - percent_educational_attainment REAL (Have columns for different levels of education)
  - percent_civilian_veteran REAL
  - percent_with_disability REAL
  - percent_born REAL (Have columns for different locations)
  - percent_citizenship_status REAL (Have columns for different statuses)
  - percent_households_with_computer REAL
  - percent_households_with_Internet REAL
  - percent_votes_democrat REAL
  - percent_votes_republican REAL
  - percent_votes_libertarian REAL
  - percent_votes_green REAL
  - percent_votes_other REAL

The fields from the social_characteristics dataset obtained from the Census Bureau in the Demographics table are not required because the Census Bureau does not have any data for these fields for counties in Puerto Rico. All other fields in all tables are required unless otherwise indicated.

## Step 3

1. We scraped 776 COVID-19 related tweets that were pre-tagged with city and state. All tweets are from the US and ~30 are not in English. Tweet date formatting wasn’t consistent with other tables. Tweet state and hashtags also needed reformatting (remove new lines, lowercase).
  
    We collected covid-related data (cases/deaths per day, vaccinations per day since January 2020) for all US counties (~3000 counties). Regarding city-to-county data (~43,000 cities), not all cities in the tweets table were represented (~30 tweets). States were abbreviated (inconsistent with other tables) and some cities spanned multiple counties (duplicate cities were present), which affected 648 tweets. Decided to consistently choose the largest county. Regarding scraped covid data, some date entries had missing FIPS ID (~13000 out of 1M entries). Vaccination date format was also inconsistent with other tables. 

    We collected demographic and political information for all US counties from Census Bureau and MIT dataset on county-level presidential election information. MIT dataset contained voting information for districts in Alaska (rather than boroughs or Census Areas), so we needed to map districts to each borough/census area.

    To create the schema from Step 2, we performed 3 joins: (1) join tweets with counties on FIPS to add fips column to the tweets table, (2) join covid cases/deaths with covid vaccinations on date to get table with all COVID-19 stats for each date, and (3) join demographics with political data on fips to get table with all the demographic/voting county data we are interested in. 

2. Your data cleaning process and outcome goes here (limit: 200 words)

- We reformatted the dates in “created_at” to a standardized Python-friendly format. 
- We removed the zip code column and consequent duplicates from the dataset mapping county to fips. Then, we mapped values in the “state” column from state abbreviations to full names. 
- As for cities spanning multiple counties, we combined county_fips with the demographics table to include a ‘total_population’ column, and chose to only include the county with the largest population for each city.
- When joining covid cases and covid vaccinations, we ignored rows in covid_cases that were missing FIPS. ~13 605 rows (out of about 1.5 million) were deleted in that process (less than 1%). We then replaced null values in the vaccination columns with 0.
- As for the join that combined tweets and counties, in order to join on state and city we performed a case-insensitive comparison between cities due to the mismatch between the tables (e.g. Los Angeles vs Los angeles). This join ignored rows where the tweets’ city was missing from the county_fips dataset. (~30 rows were deleted).
- We updated the ‘state’ column in tweets by deleting the whitespace (new line) in each record using an ‘UPDATE’ query.
- After cleaning, the tweets table had 742 rows (as opposed to 773 before cleaning). Since cleaning only resulted in the loss of around 30 rows, the lost data did not meaningfully affect the distribution.


## Step 4

We are planning on conducting two types of analyses: general demographic analysis regarding COVID misinformation and time-series analysis about how COVID misinformation spreads. Therefore, we are creating a different train-test splits for each analysis. For the former, we create a 95-5 random train-test split from our COVID tweets, taking care to keep only users and hashtags associated with the tweets in each subset; both subsets have a complete copy of the other supporting tables. For the latter, we split off data after June 1, 2021 as the test data, again taking care to keep only associated users and hashtags in each subset; however, we also split the COVID cases and vaccination table along that date for each subset. 


## Step 5

1. Generally speaking, the people whose tweets are represented here are the major stakeholders, and they are residents of the United States with mobile phone and internet access and actively using Twitter. While our work is limited to analysis, instead of inference, and thus likely has limited real-world impact, its intended goal is to help understand the spread of misinformation, which would in turn help us limit the spread of misinformation. As misinformation disproportionately harms undereducated, poorly-informed communities, which are associated with lower income brackets, such communities are therefore also major stakeholders of our work. 

2. While our work is limited to analysis, our data and techniques could be used to identify users spreading misinformation, who would likely be negatively affected by this identification; e.g., they could have their accounts flagged for suspicious activity for possibly unknowingly passing along misinformation. Moreover, we would permanently enshrine the private information individuals have disclosed on Twitter, possibly without the understanding that their tweets are public to the whole world, even after the original users delete their data. In terms of ethics problems, we are worried about the application of similar techniques to flag social media messages related to other topics, such as for the purpose of online censorship. 

3. Our data, due to its nature as tweets, suffers from sampling bias, since we can only scrape the opinion of those who have sufficient access to mobile devices and the internet. Moreover, we only filter for geotagged tweets within the US, so this further biases our data towards those with sufficiently modern mobile devices. These biases are hard to avoid, within the context of tweet analysis, but our choice to infer user location allows for a more diverse representation of users from different locations. Also, by sticking to a single dataset, which streams from Twitter using the same pipeline over a year, we avoid additional sampling bias and get a proportional representation of all geotagged COVID tweets on Twitter.  

4. There has been a number of attempts to automatically classify COVID misinformation tweets, and given that information about COVID-19 and its vaccine is politically charged within the US, such work, while important to our understanding of misinformation, has also furthered the political divide (e.g., Twitter’s automatic tagging of dubious COVID-19 information tweets has led to conservative pushback against “big tech” censorship of free speech). Our project is limited to generating insight about COVID misinformation, not inference, so the impact is likely to be much smaller. We hope that this work would allow us to be more prepared for the next misinformation pandemic and address them in more effective ways.  

## Step 6

One technical challenge we anticipate is the identification of misinformation tweets from our COVID-19 tweet dataset. We have identified various methods for identifying misinformation, including hashtag search, keyword search, and boolean search strings (i.e. fancier keyword search), but we don’t know how accurate they will be. We may also decide that it is important for our analysis to categorize tweets according to the type of COVID-19 misinformation they contain (i.e. vaccine-related, election-related, etc.). This poses a similar technical challenge, although we hope that methods like keyword search and boolean search strings will allow us to discriminate between different types of misinformation. We also anticipate challenges choosing a specific analysis. We currently have many different ideas that interest us (e.g. analyze trends in misinformation across time associated with political views or demographics or COVID-19 case count or vaccine count or analyze trends without considering time series data) but we aren’t sure how to narrow it down. 

Thus far, we have distributed work via bi-weekly meetings where tasks are assigned to each team member with the expectation that they will be completed for the next meeting a few days later. Most tasks have been individual and some have been 2-person tasks. A list of general tasks completed by each team member is as follows:

- Alex: Hunted for geotagged COVID tweet datasets and election data, helped preprocess and hydrate tweets, made train-test split, helped write report
- Megan: Hunted for covid-19 misinformation datasets, researched methods to identify misinformation tweets, helped develop database schema, helped preprocess data, processed distinct datasets into uniform database that follows the schema, helped write report
- John: Scraping and preprocessing demographic and election data, joined demographic and election data into one table, helped write report
- Nada: Scraped and preprocessed COVID-19 case and vaccination datasets, joined case and vaccination datasets into a table, scraped and cleaned city-to-county data, helped preprocess tweets table using city-to-county data, helped develop schema, helped write report

We plan to continue with a similar method for work distribution in the future, where we will meet bi-weekly to assign new tasks.
