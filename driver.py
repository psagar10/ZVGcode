from datetime import datetime
import os
import platform

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from example.example.spiders.zvg import ZvgPortalSpider

all_states = {
    'Baden-Wuerttemberg': 'bw',
    'Bavaria': 'by',
    'Berlin': 'be',
    'Brandenburg': 'br',
    'Bremen': 'hb',
    'Hamburg': 'hh',
    'Hesse': 'he',
    'Mecklenburg-Western Pomerania': 'mv',
    'Lower Saxony': 'ni',
    'North Rhine-Westphalia': 'nw',
    'Rhineland-Palatinate': 'rp',
    'Saarland': 'sl',
    'Saxony': 'sn',
    'Saxony-Anhalt': 'st',
    'Schleswig-Holstein': 'sh',
    'Thuringia': 'th'
}


def run_spider(option, download_option='no', speed='normal', stage=0):
    if stage == 0:
        print(
            f'Scrapper is running in development mode with {speed} speed.if you received any blockage,then run with '
            f'slower speed')
    else:

        print(
            f'Scrapper is running in production mode with {speed} speed.if you received any blockage,then run with '
            f'slower speed')

    if option == 'all':
        selected_states = all_states.values()
    else:
        selected_states = [all_states[option]]

    spiders = {
        "zvg": (ZvgPortalSpider, 'ZVG-Portal'),
    }

    common_settings = {
        'FEED_URI': f'{spiders["zvg"][1]}-{option.upper()}-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv',
        'FEED_FORMAT': 'csv',
        'LOG_LEVEL': 'INFO',
    }
    if stage == 0:
        common_settings.update({
            'LOG_LEVEL': 'DEBUG',
        })

    if speed == 'slow':
        common_settings.update(
            {
                'DOWNLOAD_DELAY': 2  # Custom download delay
            }
        )
    elif speed == 'normal':
        common_settings.update(
            {
                'AUTOTHROTTLE_ENABLED': True,  # Enable AutoThrottle
                'AUTOTHROTTLE_START_DELAY': 5.0,  # Initial delay in seconds
                'AUTOTHROTTLE_TARGET_CONCURRENCY': 1,  # Target request rate
                'AUTOTHROTTLE_MAX_DELAY': 60.0,  # Maximum delay in seconds
                'AUTOTHROTTLE_DEBUG': False,  # Maximum delay in seconds
            }
        )

    print(f'zvg is started with these settings\n{common_settings}')
    spiders["zvg"][0].custom_settings = common_settings
    process = CrawlerProcess(get_project_settings())
    process.crawl(spiders["zvg"][0], states=selected_states, download=download_option)
    process.start()


def choose_state_options():
    while True:
        print("Available Options: \n\t1-All States \n\t2-Specific State")
        option = input('Please select one option: ')

        available_states = all_states.keys()
        if option == '1':
            return 'all'
        elif option == '2':
            print('Available States:\n', available_states)
            while True:
                state = input('Please choose a state: ').capitalize()
                if state in available_states:
                    return state
                else:
                    print('You have selected a unknown state:Please select from the above')
        else:
            print('You have selected a wrong option.Please select again')


def choose_speed():
    while True:
        print("Please choose spider running speed: ",
              "\t1-Slow\t2-Normal\t3-Fast")
        speed = input("Enter the speed: ")
        speed = speed.lower()
        if speed in ['slow', '1']:
            return 'slow'
        elif speed in ['normal', '2']:
            return 'normal'
        elif speed in ['fast', '3']:
            return 'fast'
        else:
            "Wrong Speed.Please choose from above"


if __name__ == "__main__":
    clear = 'cls' if platform.system() in ['Windows'] else 'clear'
    _ = os.system(clear)
    print('ZVG Scrapper')
    option = choose_state_options()
    speed = choose_speed()
    download_option = input('Do you want to download documents(Yes/No)')
    run_spider(option, download_option, speed=speed, stage=1)

    print('Scraping is completed')
    input('press enter to finish')
