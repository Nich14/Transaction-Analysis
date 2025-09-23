This is a demo for transaction analysis using a [kaggle dataset]([url](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets))

ETL steps
Extraction - getting tables using KaggleHub - dim tables are complete, fact tables are only getting last n days
Translation - stripping of unneeded text, converting to required datatypes (dates, float etc)
Loading - saving to SQL - dim tables are completely overwritten, fact tables are only updated for the latest data. Error handling in case there's a gap in dates

<iframe title="Spend Analysis" width="600" height="373.5" src="https://app.powerbi.com/view?r=eyJrIjoiMTJiYTMwYTEtYzU3Zi00YjZhLWE0NWItMGUyNDI1NWZlYzIyIiwidCI6Ijk1OGY0ZmY5LTM1ZDgtNGJmMi1hN2Y3LWVmMGNmNzBlYmUzOSJ9" frameborder="0" allowFullScreen="true"></iframe>
