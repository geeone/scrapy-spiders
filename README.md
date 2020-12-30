# External functions V2

We are pushing a new version of the external functions, example spider and basic template spider files into your repo.

This is a breaking change, some things in your spiders will have to be ajusted to make them work with the new logic.

The main structural change is that all external functions are now methods of the `CustomSpider` class. Therefore, all spiders must be children of `CustomSpider` to use those as methods.

This allows our commom functions to be more tightly integrated with the spider and also to have most of the error handling logic done out of the spider, for better maintainability.

### Breaking changes
Please take a moment to check the `__init__` function in autos_prefeitura_sp and also `__init__` in `CustomSpider` at `external_functions.py`. 

As you will see all captured data now must be in `self`. The initialization is done by the `CustomSpider`. You will need to adjust your code accordingly.

---

# Our error handling policy

### Capture as much as possible
If the application finds an error in the process, but this does not prevent from getting at least part of the data, it should collect as much data as possible and return the error in the root of the payload. This is  done by including an error type and relavant payload in self.errors.

The error_type + payload should be well defined enough for us to quickly know what to look for when dealing with the problem.

### Errors already handled by the parent class

You will see that some errors are already handled in `external_functions.py`. The payload they append to `self.errors` is:

```
{"error_type": "UPLOAD_FAILED", "url": bucket_name, "data": filename, "details": str(exc)}
{"error_type": "UPLOAD_FAILED", "url": webhook_url, "data": data, "details": str(exc)}
{"error_type": "SCREENSHOT_NOT_TAKEN", "page": file_path, "details": str(exc)}
{"error_type": "CAPTCHA_FAILED", "captcha_service": captcha_service, "balance": balance, "details": str(exc)}
{"error_type": "CAPTCHA_INCORRECTLY_SOLVED", "captcha_service": captcha_service, "is_reported": is_reported}
```

### Errors to be handled by the spider
Check `autos_prefeitura_sp_spider.py` to see examples of errors that must be handled by the spider. The payload they append to `self.errors` is:
```
{"error_type": "CAPTCHA_NOT_SOLVED", "captcha_service": self.captcha_service, "details": details_msg}
{"error_type": "FILE_NOT_SAVED", "file": filename, "details": str(exc)}
```

### Scrapy errors
Please include errback=self.errback_func into all requests.

So, all calls like:
```
yield Request(url=start_url, callback=self.solve_captcha, dont_filter=True)
```
Must be changed to something like:
```
yield Request(url=start_url, callback=self.solve_captcha, errback=self.errback_func, dont_filter=True)
```
We are working on errback_func to capture the scrapy errors into our payload.

### Exception handling example
All Exceptions caught by the spider must handled like the example you see in `line 535` of `autos_prefeitura_sp` spider:
```
except Exception as exc:
    error_msg = {"error_type": "FILE_NOT_SAVED", "file": filename, "details": str(exc)}
    self.errors.append(error_msg)
    self.logger.error(error_msg)
```

---
# Utility code

### Remove diacritics + snake case conversion
The parent class now has a `remove_diacritics` function. Please use it whenever you need to dynamically convert portuguese text into snake case, it will ensure the conversion is done using exactly the same logic implemented in the javascritpt that generates our tests, and that no symbols or special characters will end up where they should not exist.

### New spider starter
Please check `example_basic_template_spider.py`. It is an updated spider starter code, already adjusted to work with the `CustomSpider`class and with some nice utility functions like `get_recaptcha_v2_token`, `get_final_result`, `download_pdf`, etc.

---

# Original onboarding document
The text bellow is exactly what you received on you first day. We kept it here just in case you need it, no changes were made.

### Should I fork this repo?
No. This is your private repo in the project. It has your name. Please commit all your work directly to it so we can follow what you are doing.

### How often should I commit?
Roughly every time you finish implementing a complete "task" for the robot. We are loosely defining "tasks" as: retrieve all requested data for a table, retrieve all fields in a page, upload a file to S3, solve a captcha, etc.

### If not using a pull request, how do I inform a spider is complete?
Commit using this exact text: `release candidate`  
We will setup our project management tool to look for these exact words and inform us that you are done.

### What happens when I finish implementing my first spider?
It will be code reviewd by an experienced spider developer. You might receive some requests to adjust things so your code better conform to our patterns. Once we are done, you will receive another spider to work on.

Please DO NOT COMMIT a release candidate without first making sure your spider passes all automated tests. The first thing we will do when we receive it for review is chek if the tests are passing. You will be saving everybody's time by checking it previously.

---

### What can be changed in this repo
Each spider should have all its crawling logic containd in one single file, placed under the `spiders` folder. Ideally, you should not need to change any other file. If changing other files or adding dependencies turns out to be required for your spider to work, please let us know so we can review and merge those changes in our main project.

### Minimize dependencies
Please avoid adding dependencies to solve simple problems, that can be solved with native python functions with not much coding complexity. External packages can be great, but they can also be one more thing to break and add complexity to deployment. We prefer to be lean on dependencies, if possible.

### Spiders naming convention
The spiders you create must be named follwoing the convention `[scraper_name]_spider`. You can find `scraper_name` in the `*-data_collected.json` file. It is the same name as thes spider's definition folder and definition pdf.


### Naming consistency
As as rule, we do not give more than one name to the same thing ANYWHERE in our projects. Ideally all variables, folders, files, arguments, parameters, etc... refering to the same thing must always have the same name in all systems, all environments, all languages. Please, keep this in mind when naming things in your code. This helps us in simplifying  code maintenance.

## Example spiders
Please take a look at the `autos_prefeitura_sp_spider.py` file. It contains an example of a spider implementation. We would greatly appreciate if you follow the coding style found in this file when creating your own crawler. Consistency will make our future maintenance efforts easier.

Also, we included the file `example_basic_template_spider.py` with more in-depth coments, where we picked the parts of the full example spider to jump start your own spider development. Please use as many of it as you find useful.

### Solving capcthas
We use two different services to solve capthas. The code needed to use them can be found in the `solve_captcha` function in the example spider file `autos_prefeitura_sp_spider.py`.

---

## How the definitions folder is structured
Each subfolder in the definitions folder represents a spider. You will find a pdf file in it with the same name as the definition folder. This pdf has all the instructions on how to retrieve data we need from the website as a **human user**. The instructions are NOT meant to represent what actions your spider must take, its purpose is to allow you to explore the website and understand what data needs to be retrieved.

Each spider definition folder will contain one or more subfolders with concrete examples that are referenced in the pdf. See bellow what each one of them means:

### *-request.json files
Contains the parameters passed to your spider when it is being executed. All other files in the same folder as a `*-request,json` file represent exactly all data that should be produced by the spider when called with those parameters. These are the parameters found in the `request` file:
  
  - scrape_id  
  Uuid generated by our backend before calling the spider. All results sent back must include the `scrape_id` as this is the only way our backend can contextualize the information and know what to do with it.
  
  - start_date  
  Not all spiders will need to handle this parameter, the definition pdf file will explain how to use it if the target website requires it. When used, this date will be in ISO complete date format (YYYY-MM-DD). Note that it will usually need to be compared to Brazilian standard date format that is found in our webpages (DD/MM/YYYY). `start_date: NULL` means: date zero, starting in the Big Bang.

  - end_date  
  Same characteristics as `start_date`. `end_date: NULL` means: date infinity, until the universe fades into oblivion.
  
  - capture_screenshot  
  Some websites - NOT ALL - require screenshot captures. We are working on the final details to define our overall scrennshot strategy. If your target website requires this, you will be informed once we finalize these definitions. **For now you don't need to do anything about it.**
  
  - get_files  
  Upload of pdf files into S3 is not required on every execution of your spider. When `get_files = true` you should upload the pdfs. If `get_files = false`, you should not.
  
  - use_proxy  
  This activates/deactivates proxy usage for each execution. It will not be hadled by the spider logic. For now, you don't need to worry about that.

  - [login parameters]: other parameters you will find in this file are used to login in the target website.


### *-data_collected.json files
When your spider finishes retrieving all data from a website, it must call the `data_collected` function from the `external_modules/external_functions` file. This function will be reponsible for sending data to the proper webhooks or saving them locally, depending on the project configuration.

Files with names ending with `-data_collected.json` represent the exact data that your spider must send to the `data_collected` function, given it was called with the parameters in the `request` json file at the same directory.

### *-upload_completed.json files
When your spider needs to upload a file to S3, it must call the `upload_completed` function from the `external_modules/external_functions` file. This function is responsible for uploading the file to S3 and informing our backend that the file was uploaded, its name and what parameters were used to build that name. In your local environment, it will save a json file with the corresponding data, allowing tests to be made.

---

## Tools
We developed a couple of small tools to try and make your development experience easier.

### Removal of diacritics and snake case converter
You will see in our documentation that we opted for using, as much as possible, the exact words found in the webpages to name things. But portuguese uses all these characters that do not usually play well in variable names. So we opted to transform all of them to snake case, removing accents and symbols, to make them more suitable for use anywhere in our services. Converting all of them manually is pain, so we created this tool to help you be more productive:  
https://brobot-snake-converter.s3-sa-east-1.amazonaws.com/index.html

Just copy whatever text you have in there and it's done.

### tests.py
To test your outputs, run `python tests.py [spider definition folder full path] [spider_name] [login_id]`  
Where `login_id` is the name of the folder containig the test files for you spider in the definitions folder. We know this is not as fancy as using a test suite with GitHub for CI, and this implementation is far from perfect. But given our time constraints, its the best we could do to simplify your testing process.

The function arguments are:
  - spider definition folder full path  
    Full path to the `definition/[spider_name]` folder.
  - spider_name  
    The name of the spider you are testing.
  - login_id  
    The same name of the subfolder under the definition folder you are testing.
  - ignore_checks (default: False)  
    DO not compare the results retrieved from the spider with the definition folder files.
  - do_not_crawl (default: False)  
    Do not run the spider.

### Acceptance tests
Your spider will only be considered completed when it passes all tests for all subfolders under your spider definition folder. We will check. Please make sure all tests are passing before commiting a release candidate.

### Errors in test files
If you think a test json file has something wrong, please let us know. We have done our best to make them as perfectly as possible, but it was done manually, so they are prone to human error.

---
### Anything is open for discussion
If you have any suggestion or complaints, please let us know.