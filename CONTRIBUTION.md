This is my first contribution MD. 
For now it will be used to collect some design thoughts. 

# For Execution 

```
python etl --config-path config/2023 --extract-stage --combine-stage --sdmx-stage
```
To only test run for certain indicators the option  --build-indicator-filter can be used. 
It either takes a pandas SQL as file or a list of SOURCE_ID in csv format.
To filter the final crba_definition as can be found in config/<year>/out/crba_report_definition.
SQL Example:=EXTRACTOR_CLASS="etl.source_adapter.csv.DefaultCSVExtractor"
```
python etl --build-indicator-filter source_filter.sql--config-path config/<crba_release_year> --extract-stage --combine-stage --sdmx-stage
```
# Use of ETL Frameworks
It should be considered to use ETL Frameworks light dagster. The goal is to reduce the code soley on the logik and less on boilerplate. 
But on the other hands Frameworks can lead to higher complexity to learn to usethe repo. 

# Thoughts about backward compatibility and other considerations
The section is not barely relevant but could help to follow the decision of the author. 
There are a few changes that might will occure in the future which can be thought about now. 

## Source vaildity over time 
There might be the case where a source is only valid for a specific period of time. 
For example Source A is used to build Indicator I. But Source A only delivers data for up to 2021. 
Afterward Source B is found to delivers the right Data. There are diffrent and possible many more options to handle this.
Some are here. 

Should ther be one method which say:

    def indicator_I():
        if target_year<=2021:
            SourceA()
        elif: target_year>2021:
            SourceB()

Or should there be a increase of degree of freedom where each config defines which sources to use. 
Or should there be a fork and *new* repo which is valid from. 

