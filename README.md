# NSQIP_Python
**A template to use Python to combine and clean NSQIP data.**

From the American College of Surgeons (ACS) National Surgical Quality Improvement Program (NSQIP®) website (*https://www.facs.org/quality-programs/data-and-registries/acs-nsqip/participant-use-data-file/*):

*"The Participant Use Data File (PUF) is a Health Insurance Portability and Accountability Act (HIPAA)-compliant data file containing cases submitted to the American College of   Surgeons National Surgical Quality Improvement Program®.
The 2023 PUF contains 994,313 cases submitted from 676 NSQIP-participating sites. Seventeen other separate NSQIP PUFs, containing a rich database of more than 11.6 million       cases, are also available. Only cases included in corresponding Semiannual Report (SAR) risk-adjustment calculations are in the PUF datasets. For background information on the   case inclusion/exclusion criteria and the data collection and submission processes, please visit the ACS NSQIP Program Specific page. Variable formats, variable definitions     (which can change from year to year), and other supporting information are available in the PUF User Guides. The data files are made available in a delimited text, SAS, and      SPSS file type."*

This repository shows how to combine both the PUF files from 2007 to 2023 and merge the targeted procedure hip files from 2016 to 2022 into a single CSV file. ACS NSQIP® provides a set of yearly, self-extracting TXT, SPSS or SAS files. This project uses TXT. The upfront effort to use a database is worth the time investment.

Of note, although you may process and clean the data using a Mac (OS), the TXT files will be sent to you from the ACS in an executable format (EXE), which requires a Windows OS to execute.

Following combining, processing, and cleaning the data, we will also save the resulting tabulated data in the form of a **Parquet** file as Parquet saves data more efficiently and preserves data types, which will be important as we will be specifying these when cleaning and processing the data.

As we approach each TXT file for processing and cleaning, it is important to note the variability in column headings and variable reporting across years. These make consolidating the data more complicated, but it can be approached methodically. I prefer to approach things piecemeal to track bugs/errors intermittently as they appear.
