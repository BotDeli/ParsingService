from utils.price import parse_price, parse_currency, format_currency
from utils.article import parse_detmir_article
from structs.product import ProductInfo
from structs.queue import LinkedQueue
from structs.task import ParsingTask
import structs.status as status
import loggers

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from random import randrange
from threading import Event
from time import sleep
import requests
import lxml


dm_s = "qa81a7ccae-3bf0-4a8c-994a-7284b23ce74a|tU2bdb0c8d-2cda-49c2-87b1-5a37d6cb7334|-N1760423883974|RK1760426889439|XJe95ee5ee54a1eb3ba0fc94e97bda3b1f6e55f7ee|Qg5aeb60b3-7b68-4f59-b372-767f4fb2fd61#cxQ22JJdDvddwWnL2IMxAlh_BOMoPJfOZZfOR5YPESQ"


geo_list = {
    "Алтай Республика": {"name": "Алтай Республика","geoCityDM": "%D0%90%D0%BB%D1%82%D0%B0%D0%B9%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-AL"},
    "Амурская область": {"name": "Амурская область", "geoCityDM": "%D0%90%D0%BC%D1%83%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-AMU"},
    "Архангельская область": {"name": "Архангельская область", "geoCityDM": "%D0%90%D1%80%D1%85%D0%B0%D0%BD%D0%B3%D0%B5%D0%BB%D1%8C%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-ARK"},
    "Астраханская область": {"name": "Астраханская область", "geoCityDM": "%D0%90%D1%81%D1%82%D1%80%D0%B0%D1%85%D0%B0%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-AST"},
    "Адыгея Республика": {"name": "Адыгея Республика", "geoCityDM": "%D0%90%D0%B4%D1%8B%D0%B3%D0%B5%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-AD"},
    "Алтайский край": {"name": "Алтайский край", "geoCityDM": "%D0%90%D0%BB%D1%82%D0%B0%D0%B9%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-ALT"},
    "Башкортостан Республика": {"name": "Башкортостан Республика", "geoCityDM": "%D0%91%D0%B0%D1%88%D0%BA%D0%BE%D1%80%D1%82%D0%BE%D1%81%D1%82%D0%B0%D0%BD%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-BA"},
    "Белгородская область": {"name": "Белгородская область", "geoCityDM": "%D0%91%D0%B5%D0%BB%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-BEL"},
    "Брянская область": {"name": "Брянская область", "geoCityDM": "%D0%91%D1%80%D1%8F%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-BRY"},
    "Бурятия Республика": {"name": "Бурятия Республика", "geoCityDM": "%D0%91%D1%83%D1%80%D1%8F%D1%82%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-BU"},
    "Владимирская область": {"name": "Владимирская область", "geoCityDM": "%D0%92%D0%BB%D0%B0%D0%B4%D0%B8%D0%BC%D0%B8%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-VLA"},
    "Волгоградская область": {"name": "Волгоградская область", "geoCityDM": "%D0%92%D0%BE%D0%BB%D0%B3%D0%BE%D0%B3%D1%80%D0%B0%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-VGG"},
    "Вологодская область": {"name": "Вологодская область", "geoCityDM": "%D0%92%D0%BE%D0%BB%D0%BE%D0%B3%D0%BE%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-VLG"},
    "Воронежская область": {"name": "Воронежская область", "geoCityDM": "%D0%92%D0%BE%D1%80%D0%BE%D0%BD%D0%B5%D0%B6%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-VOR"},
    "Дагестан Республика": {"name": "Дагестан Республика", "geoCityDM": "%D0%94%D0%B0%D0%B3%D0%B5%D1%81%D1%82%D0%B0%D0%BD%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-DA"},
    "Еврейская АО": {"name": "Еврейская АО", "geoCityDM": "%D0%95%D0%B2%D1%80%D0%B5%D0%B9%D1%81%D0%BA%D0%B0%D1%8F%20%D0%90%D0%9E", "geoCityDMIso": "RU-YEV"},
    "Забайкальский край": {"name": "Забайкальский край", "geoCityDM": "%D0%97%D0%B0%D0%B1%D0%B0%D0%B9%D0%BA%D0%B0%D0%BB%D1%8C%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-ZAB"},
    "Ивановская область": {"name": "Ивановская область", "geoCityDM": "%D0%98%D0%B2%D0%B0%D0%BD%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-IVA"},
    "Ингушетия Республика": {"name": "Ингушетия Республика", "geoCityDM": "%D0%98%D0%BD%D0%B3%D1%83%D1%88%D0%B5%D1%82%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-IN"},
    "Иркутская область": {"name": "Иркутская область", "geoCityDM": "%D0%98%D1%80%D0%BA%D1%83%D1%82%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-IRK"},
    "Кабардино-Балкарская Республика": {"name": "Кабардино-Балкарская Республика", "geoCityDM": "%D0%9A%D0%B0%D0%B1%D0%B0%D1%80%D0%B4%D0%B8%D0%BD%D0%BE-%D0%91%D0%B0%D0%BB%D0%BA%D0%B0%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KB"},
    "Калининградская область": {"name": "Калининградская область", "geoCityDM": "%D0%9A%D0%B0%D0%BB%D0%B8%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KGD"},
    "Калмыкия Республика": {"name": "Калмыкия Республика", "geoCityDM": "%D0%9A%D0%B0%D0%BB%D0%BC%D1%8B%D0%BA%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KL"},
    "Калужская область": {"name": "Калужская область", "geoCityDM": "%D0%9A%D0%B0%D0%BB%D1%83%D0%B6%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KLU"},
    "Камчатский край": {"name": "Камчатский край", "geoCityDM": "%D0%9A%D0%B0%D0%BC%D1%87%D0%B0%D1%82%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-KAM"},
    "Карачаево-Черкесская Республика": {"name": "Карачаево-Черкесская Республика", "geoCityDM": "%D0%9A%D0%B0%D1%80%D0%B0%D1%87%D0%B0%D0%B5%D0%B2%D0%BE-%D0%A7%D0%B5%D1%80%D0%BA%D0%B5%D1%81%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KC"},
    "Карелия Республика": {"name": "Карелия Республика", "geoCityDM": "%D0%9A%D0%B0%D1%80%D0%B5%D0%BB%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KR"},
    "Кемеровская область": {"name": "Кемеровская область", "geoCityDM": "%D0%9A%D0%B5%D0%BC%D0%B5%D1%80%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KEM"},
    "Кировская область": {"name": "Кировская область", "geoCityDM": "%D0%9A%D0%B8%D1%80%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KIR"},
    "Коми Республика": {"name": "Коми Республика", "geoCityDM": "%D0%9A%D0%BE%D0%BC%D0%B8%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KO"},
    "Костромская область": {"name": "Костромская область", "geoCityDM": "%D0%9A%D0%BE%D1%81%D1%82%D1%80%D0%BE%D0%BC%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KOS"},
    "Краснодарский край": {"name": "Краснодарский край", "geoCityDM": "%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D0%B4%D0%B0%D1%80%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-KDA"},
    "Красноярский край": {"name": "Красноярский край", "geoCityDM": "%D0%9A%D1%80%D0%B0%D1%81%D0%BD%D0%BE%D1%8F%D1%80%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-KYA"},
    "Курганская область": {"name": "Курганская область", "geoCityDM": "%D0%9A%D1%83%D1%80%D0%B3%D0%B0%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KGN"},
    "Курская область": {"name": "Курская область", "geoCityDM": "%D0%9A%D1%83%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-KRS"},
    "Липецкая область": {"name": "Липецкая область", "geoCityDM": "%D0%9B%D0%B8%D0%BF%D0%B5%D1%86%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-LIP"},
    "Москва и Московская область": {"name": "Москва и Московская область", "geoCityDM": "%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%20%D0%B8%20%D0%9C%D0%BE%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-MOW"},
    "Марий Эл Республика": {"name": "Марий Эл Республика", "geoCityDM": "%D0%9C%D0%B0%D1%80%D0%B8%D0%B9%20%D0%AD%D0%BB%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-ME"},
    "Мордовия Республика": {"name": "Мордовия Республика", "geoCityDM": "%D0%9C%D0%BE%D1%80%D0%B4%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-MO"},
    "Магаданская область": {"name": "Магаданская область", "geoCityDM": "%D0%9C%D0%B0%D0%B3%D0%B0%D0%B4%D0%B0%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-MAG"},
    "Мурманская область": {"name": "Мурманская область", "geoCityDM": "%D0%9C%D1%83%D1%80%D0%BC%D0%B0%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-MUR"},
    "Нижегородская область": {"name": "Нижегородская область", "geoCityDM": "%D0%9D%D0%B8%D0%B6%D0%B5%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-NIZ"},
    "Новгородская область": {"name": "Новгородская область", "geoCityDM": "%D0%9D%D0%BE%D0%B2%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-NGR"},
    "Новосибирская область": {"name": "Новосибирская область", "geoCityDM": "%D0%9D%D0%BE%D0%B2%D0%BE%D1%81%D0%B8%D0%B1%D0%B8%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-NVS"},
    "Омская область": {"name": "Омская область", "geoCityDM": "%D0%9E%D0%BC%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-OMS"},
    "Оренбургская область": {"name": "Оренбургская область", "geoCityDM": "%D0%9E%D1%80%D0%B5%D0%BD%D0%B1%D1%83%D1%80%D0%B3%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-ORE"},
    "Орловская область": {"name": "Орловская область", "geoCityDM": "%D0%9E%D1%80%D0%BB%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-ORL"},
    "Пензенская область": {"name": "Пензенская область", "geoCityDM": "%D0%9F%D0%B5%D0%BD%D0%B7%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-PNZ"},
    "Пермский край": {"name": "Пермский край", "geoCityDM": "%D0%9F%D0%B5%D1%80%D0%BC%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-PER"},
    "Псковская область": {"name": "Псковская область", "geoCityDM": "%D0%9F%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-PSK"},
    "Приморский край": {"name": "Приморский край", "geoCityDM": "%D0%9F%D1%80%D0%B8%D0%BC%D0%BE%D1%80%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-PRI"},
    "Ростовская область": {"name": "Ростовская область", "geoCityDM": "%D0%A0%D0%BE%D1%81%D1%82%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-ROS"},
    "Рязанская область": {"name": "Рязанская область", "geoCityDM": "%D0%A0%D1%8F%D0%B7%D0%B0%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-RYA"},
    "Самарская область": {"name": "Самарская область", "geoCityDM": "%D0%A1%D0%B0%D0%BC%D0%B0%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SAM"},
    "Санкт-Петербург и Ленинградская область": {"name": "Санкт-Петербург и Ленинградская область", "geoCityDM": "%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3%20%D0%B8%20%D0%9B%D0%B5%D0%BD%D0%B8%D0%BD%D0%B3%D1%80%D0%B0%D0%B4%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SPE"},
    "Саратовская область": {"name": "Саратовская область", "geoCityDM": "%D0%A1%D0%B0%D1%80%D0%B0%D1%82%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SAR"},
    "Саха Республика - Якутия": {"name": "Саха Республика - Якутия", "geoCityDM": "%D0%A1%D0%B0%D1%85%D0%B0%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0%20-%20%D0%AF%D0%BA%D1%83%D1%82%D0%B8%D1%8F", "geoCityDMIso": "RU-SA"},
    "Сахалинская область": {"name": "Сахалинская область", "geoCityDM": "%D0%A1%D0%B0%D1%85%D0%B0%D0%BB%D0%B8%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SAK"},
    "Северная Осетия - Алания Республика": {"name": "Северная Осетия - Алания Республика", "geoCityDM": "%D0%A1%D0%B5%D0%B2%D0%B5%D1%80%D0%BD%D0%B0%D1%8F%20%D0%9E%D1%81%D0%B5%D1%82%D0%B8%D1%8F%20-%20%D0%90%D0%BB%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-SE"},
    "Свердловская область": {"name": "Свердловская область", "geoCityDM": "%D0%A1%D0%B2%D0%B5%D1%80%D0%B4%D0%BB%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SVE"},
    "Смоленская область": {"name": "Смоленская область", "geoCityDM": "%D0%A1%D0%BC%D0%BE%D0%BB%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-SMO"},
    "Ставропольский край": {"name": "Ставропольский край", "geoCityDM": "%D0%A1%D1%82%D0%B0%D0%B2%D1%80%D0%BE%D0%BF%D0%BE%D0%BB%D1%8C%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-STA"},
    "Тамбовская область": {"name": "Тамбовская область", "geoCityDM": "%D0%A2%D0%B0%D0%BC%D0%B1%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-TAM"},
    "Тверская область": {"name": "Тверская область", "geoCityDM": "%D0%A2%D0%B2%D0%B5%D1%80%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-TVE"},
    "Томская область": {"name": "Томская область", "geoCityDM": "%D0%A2%D0%BE%D0%BC%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-TOM"},
    "Тульская область": {"name": "Тульская область", "geoCityDM": "%D0%A2%D1%83%D0%BB%D1%8C%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-TUL"},
    "Тыва Республика": {"name": "Тыва Республика", "geoCityDM": "%D0%A2%D1%8B%D0%B2%D0%B0%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-TY"},
    "Тюменская область": {"name": "Тюменская область", "geoCityDM": "%D0%A2%D1%8E%D0%BC%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-TYU"},
    "Татарстан Республика": {"name": "Татарстан Республика", "geoCityDM": "%D0%A2%D0%B0%D1%82%D0%B0%D1%80%D1%81%D1%82%D0%B0%D0%BD%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-TA"},
    "Удмуртская Республика": {"name": "Удмуртская Республика", "geoCityDM": "%D0%A3%D0%B4%D0%BC%D1%83%D1%80%D1%82%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-UD"},
    "Ульяновская область": {"name": "Ульяновская область", "geoCityDM": "%D0%A3%D0%BB%D1%8C%D1%8F%D0%BD%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-ULY"},
    "Хабаровский край": {"name": "Хабаровский край", "geoCityDM": "%D0%A5%D0%B0%D0%B1%D0%B0%D1%80%D0%BE%D0%B2%D1%81%D0%BA%D0%B8%D0%B9%20%D0%BA%D1%80%D0%B0%D0%B9", "geoCityDMIso": "RU-KHA"},
    "Хакасия Республика": {"name": "Хакасия Республика", "geoCityDM": "%D0%A5%D0%B0%D0%BA%D0%B0%D1%81%D0%B8%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-KK"},
    "Ханты-Мансийский Автономный округ - Югра АО": {"name": "Ханты-Мансийский Автономный округ - Югра АО.", "geoCityDM": "%D0%A5%D0%B0%D0%BD%D1%82%D1%8B-%D0%9C%D0%B0%D0%BD%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%B8%D0%B9%20%D0%90%D0%B2%D1%82%D0%BE%D0%BD%D0%BE%D0%BC%D0%BD%D1%8B%D0%B9%20%D0%BE%D0%BA%D1%80%D1%83%D0%B3%20-%20%D0%AE%D0%B3%D1%80%D0%B0%20%D0%90%D0%9E.", "geoCityDMIso": "RU-KHM"},
    "Челябинская область": {"name": "Челябинская область", "geoCityDM": "%D0%A7%D0%B5%D0%BB%D1%8F%D0%B1%D0%B8%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-CHE"},
    "Чеченская Республика": {"name": "Чеченская Республика", "geoCityDM": "%D0%A7%D0%B5%D1%87%D0%B5%D0%BD%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0", "geoCityDMIso": "RU-CE"},
    "Чувашская Республика - Чувашия": {"name": "Чувашская Республика - Чувашия.", "geoCityDM": "%D0%A7%D1%83%D0%B2%D0%B0%D1%88%D1%81%D0%BA%D0%B0%D1%8F%20%D0%A0%D0%B5%D1%81%D0%BF%D1%83%D0%B1%D0%BB%D0%B8%D0%BA%D0%B0%20-%20%D0%A7%D1%83%D0%B2%D0%B0%D1%88%D0%B8%D1%8F.", "geoCityDMIso": "RU-CU"},
    "Ямало-Ненецкий АО": {"name": "Ямало-Ненецкий АО.", "geoCityDM": "%D0%AF%D0%BC%D0%B0%D0%BB%D0%BE-%D0%9D%D0%B5%D0%BD%D0%B5%D1%86%D0%BA%D0%B8%D0%B9%20%D0%90%D0%9E.", "geoCityDMIso": "RU-YAN"},
    "Ярославская область": {"name": "Ярославская область", "geoCityDM": "%D0%AF%D1%80%D0%BE%D1%81%D0%BB%D0%B0%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C", "geoCityDMIso": "RU-YAR"},
}

location_names = {
    "Горно-Алтайск": "Алтай Республика",
    "Благовещенск": "Амурская область",
    "Архангельск": "Архангельская область",
    "Астрахань": "Астраханская область",
    "Майкоп": "Адыгея Республика",
    "Барнаул": "Алтайский край",
    "Уфа": "Башкортостан Республика",
    "Белгород": "Белгородская область",
    "Брянск": "Брянская область",
    "Улан-Удэ": "Бурятия Республика",
    "Владимир": "Владимирская область",
    "Волгоград": "Волгоградская область",
    "Вологда": "Вологодская область",
    "Воронеж": "Воронежская область",
    "Махачкала": "Дагестан Республика",
    "Биробиджан": "Еврейская АО",
    "Чита": "Забайкальский край",
    "Иваново": "Ивановская область",
    "Магас": "Ингушетия Республика",
    "Иркутск": "Иркутская область",
    "Нальчик": "Кабардино-Балкарская Республика",
    "Калининград": "Калининградская область",
    "Элиста": "Калмыкия Республика",
    "Калуга": "Калужская область",
    "Петропавловск-Камчатский": "Камчатский край",
    "Черкесск": "Карачаево-Черкесская Республика",
    "Петрозаводск": "Карелия Республика",
    "Кемерово": "Кемеровская область",
    "Киров": "Кировская область",
    "Сыктывкар": "Коми Республика",
    "Кострома": "Костромская область",
    "Краснодар": "Краснодарский край",
    "Красноярск": "Красноярский край",
    "Курган": "Курганская область",
    "Курск": "Курская область",
    "Липецк": "Липецкая область",
    "Москва": "Москва и Московская область",
    "Йошкар-Ола": "Марий Эл Республика",
    "Саранск": "Мордовия Республика",
    "Магадан": "Магаданская область",
    "Мурманск": "Мурманская область",
    "Нижний Новгород": "Нижегородская область",
    "Великий Новгород": "Новгородская область",
    "Новосибирск": "Новосибирская область",
    "Омск": "Омская область",
    "Оренбург": "Оренбургская область",
    "Орёл": "Орловская область",
    "Пенза": "Пензенская область",
    "Пермь": "Пермский край",
    "Псков": "Псковская область",
    "Владивосток": "Приморский край",
    "Ростов-на-Дону": "Ростовская область",
    "Рязань": "Рязанская область",
    "Самара": "Самарская область",
    "Санкт-Петербург": "Санкт-Петербург и Ленинградская область",
    "Саратов": "Саратовская область",
    "Якутск": "Саха Республика - Якутия",
    "Южно-Сахалинск": "Сахалинская область",
    "Владикавказ": "Северная Осетия - Алания Республика",
    "Екатеринбург": "Свердловская область",
    "Смоленск": "Смоленская область",
    "Ставрополь": "Ставропольский край",
    "Тамбов": "Тамбовская область",
    "Тверь": "Тверская область",
    "Томск": "Томская область",
    "Тула": "Тульская область",
    "Кызыл": "Тыва Республика",
    "Тюмень": "Тюменская область",
    "Казань": "Татарстан Республика",
    "Ижевск": "Удмуртская Республика",
    "Ульяновск": "Ульяновская область",
    "Хабаровск": "Хабаровский край",
    "Абакан": "Хакасия Республика",
    "Ханты-Мансийск": "Ханты-Мансийский Автономный округ - Югра АО",
    "Челябинск": "Челябинская область",
    "Грозный": "Чеченская Республика",
    "Чебоксары": "Чувашская Республика - Чувашия",
    "Салехард": "Ямало-Ненецкий АО",
    "Ярославль": "Ярославская область"
}

class DetmirParser():
    def __init__(self, cfg: dict):
        all_cfg = cfg["all"]
        self.cycle_min_cooldown = all_cfg["cycle_min_cooldown_ms"]
        self.cycle_max_cooldown = all_cfg["cycle_max_cooldown_ms"]
        self.request_min_cooldown = all_cfg["request_min_cooldown_ms"]
        self.request_max_cooldown = all_cfg["request_max_cooldown_ms"]
        
        self.parsing_queue = LinkedQueue()

        self.ua = UserAgent()
        self._init_session()
        
    def _init_session(self):
        self.session = requests.session()
        self.session.headers.update({
            "Accept": "*/*",
            "Connection": "keep-alive",
            "User-Agent": self.ua.random,
            "Upgrade-Insecure-Requests": "1"
        })

    def start_listen_parsing_queue(self, stop_event: Event):
        while not stop_event.is_set():
            if self.parsing_queue.is_next():
                task = None
                try:
                    task = self.parsing_queue.pop()
                    self._handle_parsing_task(task, stop_event)
                except Exception as error:
                    if not task is None:
                        task.on_error("detmir")

                    loggers.detmir.warning(f"Skip panic parsing task: {error}")

            self._cycle_cooldown()
        
        self.session.close()

    def _handle_parsing_task(self, task: ParsingTask, stop_event: Event):
        geo_params = task.geo_params
        url = task.url
        products = []

        for location in geo_params:
            if stop_event.is_set():
                return

            try:
                product = self.parse_product_with_location(url, location)
                products.append(product)
                self._request_cooldown()
            except:
                pass

        if not stop_event.is_set():
            task.callback(products)

    def _request_cooldown(self):
        cooldown_ms = randrange(self.request_min_cooldown, self.request_max_cooldown)
        sleep(cooldown_ms / 1000)

    def _cycle_cooldown(self):
        cooldown_ms = randrange(self.cycle_min_cooldown, self.cycle_max_cooldown)
        sleep(cooldown_ms / 1000)

    def add_to_parsing_queue(self, task: ParsingTask):
        self.parsing_queue.push(task)

    def parse_product_with_location(self, url: str, location: str) -> ProductInfo:
        product = ProductInfo(url, "detmir")
        product.set_location(location)

        article = parse_detmir_article(url)
        product.set_article(article)

        if location not in location_names.keys():
            product.set_status(status.ERR_FAIL, -1)
            product.set_error("fail find geo data", "location not found")
            return product
        
        geo_name = location_names[location]
        geo_data = geo_list[geo_name]
        
        try:
            r = self.session.get(url, cookies={
                "geoCityDM": geo_data["geoCityDM"],
                "geoCityDMCode": "",
                "geoCityDMIso": geo_data["geoCityDMIso"]
            })
            soup = BeautifulSoup(r.text, "lxml")
        except requests.Timeout as err:
            product.set_status(status.ERR_TIMEOUT, -1)
            product.set_error("fail parse product", err)
            return product
        except Exception as err:
            product.set_status(status.ERR_FAIL, -1)
            product.set_error("fail parse product", err)
            return product

        el_price_block = soup.find('section', attrs={'data-testid': 'priceBlock'})
        if el_price_block is None:
            product.set_status(status.ERR_CAPTCHA, r.status_code)
            product.set_error("error load html page", "anti-bot defender")
            return product
        
        el_price = el_price_block.find('p', attrs={'data-testid': 'price'})
        if el_price is None:
            product.set_status(status.PARSING_SUCCESS, r.status_code)
            return product
        
        price = parse_price(el_price.text)
        currency = parse_currency(el_price.text)
        currency = format_currency(currency)
       
        original_price = price
        el_original_price = el_price.find_next('p').text
        if not el_original_price is None:
            original_price = parse_price(el_original_price)

        product.set_aviable(True)
        product.set_price_data(price, original_price, currency)
        product.set_status(status.PARSING_SUCCESS, r.status_code)

        return product

