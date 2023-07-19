# An end-to-end data pipeline for housing market analysis using Rightmove

## Extract

Webscraping using Selenium in python.

- Class `Data` used to store the data.
    - Method `add_page_data(driver)` used to add data from the current webpage, given the selenium `driver` (using Chrome driver).

Currently we are scraping from this base url: <https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E92829&maxBedrooms=2&minBedrooms=2&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=>  
This corresponds to 2 bed properties in South-West London available to rent, which defines the properties of interest for this problem.

We are currently scraping 3 pieces of data for each property using Selenium:

1. Property id (natural key). You should always make sure you have a way to uniquely identify an entity (i.e. property) to avoid duplication. In this case it is certainly important as some properties are 'featured' and can appear on multiple pages meaning their data will be scraped multiple times. Having property id data means we can enforce a uniqueness constraint on the id in our RDMS to avoid duplication on loading. This piece of data is found in the "id" attribute of \<a> tags for each property with the class name "propertyCard-anchor".
2. Price. This is the monthly rental cost of the property. It can be found in the text of the \<span> tags with the class name "propertCard-priceValue".
3. Address. This is the address of the property. It can be found in the "title" attribute of the \<address> tags with the class name "propertCard-address".

In order to scrape the data using Selenium, we use the Chrome driver (through Selenium) to interact with the webpage. First, we may need to accept cookies so we have added a function `accept_cookies_if_asked()` to find the button on the pop-up up by id, if it exists, and click.

We also need to click through all available pages of properties to make sure we have the complete dataset. To do this, we search for the button using:

```python
button = driver.find_element(By.XPATH, "//button[@title='Next page']")
```

This searches for a button element with title attribute "Next page". Which is the one we want on Rightmove! Once we have found that we can click the button to navaigate to the next page

```python
button.click()
```

We carry out this process in a while loop through all pages with the handy condition `button.is_enabled()`, which we can use as on the final page the button still exists in the html but is disabled.

Due to the nature of the driver. We need to enforce some waits in to our code, specifically we need to wait until the 'next page' button is clickable (loaded) before trying to click it:

```python
WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
```

and we need to wait until the html elements we need are available (loaded) before trying to scrape them:

```python
WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'propertyCard-anchor')))
```

## Transform

- The price is in the text form "£x,xxx pcm" where x are numbers. We add a method to our `Data` class, `.transform_prices()` which transforms the `prices` attribute from a list of strings to a list of integers (without "£", " pcm" and commas).
- We add a method `make_df` to the `Data` class which returns a dataframe of all of the data. This can then be used to load into the database.
