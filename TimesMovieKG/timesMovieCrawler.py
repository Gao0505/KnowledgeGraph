# -*- coding: utf-8 -*-
# @Time    : 2019/8/13 14:37
# @Author  : Gao
# @File    : timesMovieCrawler.py

import requests
from bs4 import BeautifulSoup
import pandas as pd

DOWNLOAD_URL = 'http://www.mtime.com/top/movie/top100/'  # the download url

'''
    FunctionName: download_page
    Purpose: download webpage
    Parameter: 
                1. url[string]: the url of webpage

                2. headers[string]: disguised as a browser
    Return: the content of url, i.e., html data
    Remark: use headers to avoid being intercepted by anti-reptile mechanism
'''


def download_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36'}
    return requests.get(url, headers=headers).content


'''
    FunctionName: parse_timesmovie_html
    Purpose: parse webpage with BeautifuSoup from times movie website
    Parameter: 
                1. html[string]: the html data of webpage

                2. soup[bs4.BeautifulSoup]: save the parse result with tool BeautifulSoup
                3. movie_list[list]: save list of movie 
    Return: movie_list
    Remark: all the information of movie: rank, src, movie_name, movie_en_name, year, director, actor 
'''


def parse_timesmovie_html(html):
    soup = BeautifulSoup(html, features="lxml")  # parse tool: BeautifulSoup
    movie_list = []  # movie_node list: sava all the movie info into the list
    movie_list_soup = soup.find('ul', attrs={'id': 'asyncRatingRegion'})  # find body
    # get the data of: rank, movie_name, src, director, movie_actor[]; then save in the
    for movie_li in movie_list_soup.find_all('li'):
        rank = movie_li.find('div', attrs={'class': 'number'}).getText()  # rank of movie
        movie_info = movie_li.find('div', attrs={'class': 'mov_con'})
        movie_temp = movie_info.find('a').getText().split("\xa0")  # name of movie
        movie_name = movie_temp[0]
        year = movie_temp[1].split(" ")[-1].replace('(', '').replace(')', '')  # release date of movie
        movie_en_name = movie_temp[1][:-7]  # English name of movie
        # print(movie_en_name)

        src = movie_info.find('a')['href']  # src of movie
        # extract the infomation of director&actor
        person_info = movie_info.select('p')
        # director of movie
        director = person_info[0].find('a').string
        # actors of movie
        movie_actor = []
        for act in person_info[1].find_all('a'):
            movie_actor.append(act.string)

        movie_info_all = {'rank': rank,
                          'src': src,
                          'movie_name': movie_name,
                          'movie_en_name': movie_en_name,
                          'year': year,
                          'director': director,
                          'actor': movie_actor
                          }
        movie_list.append(movie_info_all)
    return movie_list


'''
    FunctionName: handle_entity
    Purpose: Handling entities that may be duplicated: director& actors
            #处理可能会重复的实体：导演和演员
    Parameter: 
                1. person_list[list]: list with information of director& actors

                2. director_list[list]: list of director
                3. actor_list[list]: list of actors 
                4. dir_dic[dict]: dictionary of director
                5. actor_dic[dict]: dictionary of actors
    Return: dir_dic
            actor_dic
    Remark: 
            1. the set() is utilize to remove duplicate data from the list[].
            2. elements of dictionary are not repeated and the form of each data is {key: value}({int, string}) 
'''


def handle_entity(person_list):
    director_list, actor_list = [], []
    dir_dic, actor_dic = {}, {}

    # separate infomation from person_list
    for elm in person_list:
        director_list.append(elm['director'])
        actor_list.extend(elm['actor'])
    # utilize set() to remove duplicate data from the list[]
    director_set = set(director_list)
    actor_set = set(actor_list)
    # save data to the dictionary
    for index, value in enumerate(director_set):
        dir_dic[index] = value
    for index, value in enumerate(actor_set):
        actor_dic[index] = value
    # print(dir_dic)
    return dir_dic, actor_dic


'''
    FunctionName: 
                1. save_movie_info
                2. save_movie_entity
                3. save_director_entity
                4. save_actor_entity
    Purpose: save movie information and eneity information to the csv file
    Parameter: 
                1. movie_data_list[list]: list of movie information
                2. movie_entity_list[list]: list of movie entities                
                3. director_entity_dic[dict]: dictionary of director entity
                4. actor_entity_dic[dict]: dictionary of actor entity
    Return: none
    Remark: get files:
            times_movies_info.csv; 
            times_movie_entity.csv; 
            times_director_entity.csv; 
            times_actor_entity.csv
'''


def save_movie_info(movie_data_list):
    data = pd.DataFrame({'rank': [movie['rank'] for movie in movie_data_list],
                         'src': [movie['src'] for movie in movie_data_list],
                         'movie_name': [movie['movie_name'] for movie in movie_data_list],
                         'movie_en_name': [movie['movie_en_name'] for movie in movie_data_list],
                         'year': [movie['year'] for movie in movie_data_list],
                         'director': [movie['director'] for movie in movie_data_list],
                         'actor': [movie['actor'] for movie in movie_data_list]
                         })
    data.to_csv(r'./times_movies_info.csv', index=False)


def save_movie_entity(movie_entity_list):
    data = pd.DataFrame({'index:ID': [index + 1000 for index in range(len(movie_entity_list))],
                         'rank': [movie['rank'] for movie in movie_entity_list],
                         'src': [movie['src'] for movie in movie_entity_list],
                         'name': [movie['movie_name'] for movie in movie_entity_list],
                         'movie_en': [movie['movie_en_name'] for movie in movie_entity_list],
                         'year': [movie['year'] for movie in movie_entity_list],
                         ':LABEL': 'movie'
                         })
    data.to_csv(r'./times_movie_entity.csv', index=False)


def save_director_entity(director_entity_dic):
    data = pd.DataFrame({'index:ID': [item + 2000 for item in director_entity_dic.keys()],
                         'director': [val for val in director_entity_dic.values()],
                         ':LABEL': 'director'
                         })
    data.to_csv(r'./times_director_entity.csv', index=False)


def save_actor_entity(actor_entity_dic):
    data = pd.DataFrame({'index:ID': [item + 3000 for item in actor_entity_dic.keys()],
                         'actor': [val for val in actor_entity_dic.values()],
                         ':LABEL': 'actor'
                         })
    data.to_csv(r'./times_actor_entity.csv', index=False)


'''
    FunctionName: 
                1. save_movie_director_relationship
                2. save_movie_actor_relationship
                3. save_director_actor_relationship
    Purpose: save relationship information to the csv file
    Parameter: 
                1. movie_list[list]: list of movie information
                2. movie_dic[list]: dictionary of movie entities                
                3. director_dic[dict]: dictionary of director entity
                4. actor_dict[dict]: dictionary of actor entity
    Return: none
    Remark: get files:
            times_movie_director_relationship.csv; 
            times_movie_actor_relationship.csv; 
            times_director_actor_relationship.csv
'''


def save_movie_director_relationship(movie_list, movie_dic, director_dic):
    data = pd.DataFrame({':START_ID': [
        list(director_dic.keys())[list(director_dic.values()).index(info['director'])] + 2000 for info in movie_list],
                         ':END_ID': [list(movie_dic.keys())[list(movie_dic.values()).index(info['movie_name'])] for info
                                     in movie_list],
                         'relation': 'directed',
                         ':TYPE': 'directed'
                         })
    data.to_csv(r'./times_movie_director_relationship.csv', index=False)


def save_movie_actor_relationship(movie_list, movie_dic, actor_dict):
    relation_list = []
    for movie in movie_list:
        for actor_info in movie['actor']:
            actor_index = list(actor_dict.keys())[list(actor_dict.values()).index(actor_info)]
            movie_index = list(movie_dic.keys())[list(movie_dic.values()).index(movie['movie_name'])]
            info = {
                'actor_index': actor_index,
                'movie_index': movie_index
            }
            relation_list.append(info)

    data = pd.DataFrame({':START_ID': [relation['actor_index'] + 3000 for relation in relation_list],
                         ':END_ID': [relation['movie_index'] for relation in relation_list],
                         'relation': 'acted',
                         ':TYPE': 'acted'
                         })
    data.to_csv(r'./times_movie_actor_relationship.csv', index=False)


def save_director_actor_relationship(movie_list, director_dic, actor_dict):
    relation_list = []
    for movie in movie_list:
        for actor_info in movie['actor']:
            actor_index = list(actor_dict.keys())[list(actor_dict.values()).index(actor_info)]
            dir_index = list(director_dic.keys())[list(director_dic.values()).index(movie['director'])]
            info = {
                'actor_index': actor_index,
                'dir_index': dir_index
            }
            relation_list.append(info)

    data = pd.DataFrame({':START_ID': [relation['dir_index'] + 2000 for relation in relation_list],
                         ':END_ID': [relation['actor_index'] + 3000 for relation in relation_list],
                         'relation': 'cooperate',
                         ':TYPE': 'cooperate'
                         })
    data.to_csv(r'./times_director_actor_relationship.csv', index=False)


def main():
    print("----------------------start------------------------")
    page_url = DOWNLOAD_URL

    # traverse all pages
    html_data = download_page(page_url)
    movie_list = parse_timesmovie_html(html_data)
    for num in range(2, 11):
        page_index = 'index-' + str(num) + '.html'
        page_url = DOWNLOAD_URL + page_index
        html_data = download_page(page_url)
        lists = parse_timesmovie_html(html_data)
        movie_list.extend(lists)
    dir_dict, actor_dict = handle_entity(movie_list)

    print("--------start saving entity information to csv files--------")
    # save_movie_info(movie_list)
    save_movie_entity(movie_list)
    save_director_entity(dir_dict)
    save_actor_entity(actor_dict)
    print("--------------------save over--------------------")

    print("--------start saving relationship information to csv files--------")
    # save data to the movie dictionary
    movie_dic = {}
    for movies in movie_list:
        movie_dic[int(movies['rank']) + 999] = movies['movie_name']
    # print(movie_dic)

    save_movie_director_relationship(movie_list, movie_dic, dir_dict)
    save_movie_actor_relationship(movie_list, movie_dic, actor_dict)
    save_director_actor_relationship(movie_list, dir_dict, actor_dict)
    print("--------------------save over--------------------")

    print("-----------------------end-------------------------")


if __name__ == '__main__':
    main()