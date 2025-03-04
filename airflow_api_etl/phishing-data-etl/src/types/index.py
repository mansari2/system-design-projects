from typing import List, Dict, Any

class PhishingRecord:
    def __init__(self, 
                 region_name: str,
                 city: str,
                 zipcode: str,
                 latitude: float,
                 longitude: float,
                 geohash: str,
                 asn: str,
                 bgp: str,
                 isp: str,
                 title: str,
                 detection_date: str,
                 update_date: str,
                 hash: str,
                 score: float,
                 host: str,
                 domain: str,
                 tld: str,
                 domain_age_days: int):
        self.region_name = region_name
        self.city = city
        self.zipcode = zipcode
        self.latitude = latitude
        self.longitude = longitude
        self.geohash = geohash
        self.asn = asn
        self.bgp = bgp
        self.isp = isp
        self.title = title
        self.detection_date = detection_date
        self.update_date = update_date
        self.hash = hash
        self.score = score
        self.host = host
        self.domain = domain
        self.tld = tld
        self.domain_age_days = domain_age_days

class Metadata:
    def __init__(self, 
                 hash: str,
                 screenshot_url: str,
                 abuse_contact: str,
                 ssl_issuer: str,
                 ssl_subject: str,
                 alexa_rank_host: int,
                 alexa_rank_domain: int,
                 times_seen_ip: int,
                 times_seen_host: int,
                 times_seen_domain: int,
                 http_code: int,
                 http_server: str,
                 google_safebrowsing: str,
                 virus_total: str,
                 abuse_ch_malware: str):
        self.hash = hash
        self.screenshot_url = screenshot_url
        self.abuse_contact = abuse_contact
        self.ssl_issuer = ssl_issuer
        self.ssl_subject = ssl_subject
        self.alexa_rank_host = alexa_rank_host
        self.alexa_rank_domain = alexa_rank_domain
        self.times_seen_ip = times_seen_ip
        self.times_seen_host = times_seen_host
        self.times_seen_domain = times_seen_domain
        self.http_code = http_code
        self.http_server = http_server
        self.google_safebrowsing = google_safebrowsing
        self.virus_total = virus_total
        self.abuse_ch_malware = abuse_ch_malware

PhishingRecordsList = List[PhishingRecord]
MetadataList = List[Metadata]