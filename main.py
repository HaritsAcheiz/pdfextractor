from scraper import SeasonalJobsScraper

jobs_load_endpoint = 'https://api.seasonaljobs.dol.gov/datahub/search?api-version=2020-06-30'

scraper = SeasonalJobsScraper()
scraper.main()
