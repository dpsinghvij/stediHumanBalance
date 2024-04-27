# STEDI Human Balance Analytics

Stedi Human Balance Analytics uses Glue Spark job to process the accelerometer data.

Following data was imported from Udacity project directory
- customer
- accelerometer
- step trainer


Project flow
1. The data from the sources is saved in S3 in landing directories
2. The customer landing data is then transferred to customer trusted folder after filtering out the users that didn't request data to be shared for research.
3. The accelerometer landing data is joined with customer trusted data so that  accelerometer trusted just have records of users who have agreed to share the data for research
4. Customer trusted and accelerometer trusted are joined and distinct records are used as customer curated
5. Step trainer records and customer curated records are joined on serialnumber to use the records of the people that have agreed to share the data.
6. Machine learning curated data is created after joining curated data of step trainer, customers, and accelerometer


Project Structure
- The Python scripts can be found in the "script" folder
- Screenshots of the data is in "screenshots" folder
- Sql table create statements can be found in "sql" folder