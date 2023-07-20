# An end-to-end data pipeline for housing market analysis using Rightmove

Problem/business statement: We want to extract data from Rightmove.com to analyse the trends in the rental costs of 2-bed properties in South West London. In particular, we want to analyse trends over time and geographically within the SW London area.

In this project, I have used Selenium to get data from the Rightmove webpage. I then load it into a relational database (SQL Server on AWS RDS) with a normalised, orthogonal data maodel. Following this, I will use the data in the database to build a dashboard for analysis.

## Extract

Webscraping using Selenium in python.

- Class `Data` used to temporarily store the data upon extraction and hold all the necessary functionality to tranform the data before loading into our relational database.
    - Method `add_page_data(driver)` used to add data from the current webpage, given the selenium `driver` (using Chrome driver).

Currently we are scraping from this base url: <https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E92829&maxBedrooms=2&minBedrooms=2&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=>  
This corresponds to 2 bed properties in South-West London available to rent, which defines the properties of interest for this problem.

We are currently scraping 3 pieces of data for each property using Selenium:

1. Property id (natural key). You should always make sure you have a way to uniquely identify an entity (i.e. property) to avoid duplication. In this case it is certainly important as some properties are 'featured' and can appear on multiple pages meaning their data will be scraped multiple times. Having property id data means we can enforce a uniqueness constraint on the id in our RDMS to avoid duplication on loading. This piece of data is found in the "id" attribute of \<a> tags for each property with the class name "propertyCard-anchor".
2. Price. This is the monthly rental cost of the property. It can be found in the text of the \<span> tags with the class name "propertCard-priceValue".
3. Address. This is the address of the property. It can be found in the "title" attribute of the \<address> tags with the class name "propertCard-address".

In order to scrape the data using Selenium, we use the Chrome driver (through Selenium) to interact with the webpage. First, we may need to accept cookies so we have added a function `accept_cookies_if_asked()` to find the button on the pop-up up by id and click, if it exists and is clickable.

We also need to click through all available pages of properties to make sure we have the complete dataset. To do this, we search for the button using:

```python
button = driver.find_element(By.XPATH, "//button[@title='Next page']")
```

This searches for a button element with title attribute "Next page". Which is the one we want on Rightmove! Once we have found that, we can click the button to navaigate to the next page

```python
button.click()
```

We carry out this process in a while loop, looping through all pages with the handy condition `button.is_enabled()`. We can use this condition because, on the final page, the button still exists in the html but is disabled.

Due to the nature of the driver. We need to enforce some waits in to our code, specifically we need to wait until the 'next page' button is clickable (loaded) before trying to click it:

```python
WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
```

and we need to wait until the html elements we need are available (loaded) before trying to scrape them:

```python
WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'propertyCard-anchor')))
```

## Transform

- The price is in the text form "£X,XXX pcm" where X are numbers. We add a method to our `Data` class, `.transform_prices()` which transforms the `prices` attribute from a list of strings into a list of integers (without "£", " pcm" and commas).
- The location given on the rightmove page is inconsistent and often without a postcode :angry:. For now, we use the [bing maps api](https://learn.microsoft.com/en-us/bingmaps/rest-services/locations/find-a-location-by-query). This allows us to pass the address scraped to get info such as the postcodes. However, the api doesn't always return a postcode in the json response (maybe try a different api if it becomes a problem - google looks promising but requires card details). So far, it has seemed to always return coordinates. So we are adding postcodes where possible and adding geolocation coordiantes (latitude and longitude) to all properties. The method `transform_location_data(self)` is added to the `Data` class to fill in the geolocation attributes. We also add ' London, UK' to the address search to remove potential ambiguity.
- We add a method `make_df` to the `Data` class which returns a dataframe of all of the data. This can then be used to load into the database.

# to add:
- data: current date
- transformations: zip into list of tuples for the odbc cursor method.
- rounding coords, removing "prop" from id and turning to integer - ensure that the data adhere's to the restrictions imposed in the database
- Loading section: pyodbc, used cte and subquery to load the data into a cte and then load into the table referencing cte in subquery to ensure that we only load new data i.e. obey primary key constraint.
- Accessing the bank of england page first using requests to set cookies <https://stackoverflow.com/questions/70792547/fom-browser-a-csv-file-from-url-url-1-can-be-downloaded-only-if-another-url-of>