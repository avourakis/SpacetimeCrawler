import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager, Link
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
from io import StringIO
import re, os
from time import time

subdomaincount = dict() #keep track of all subdomains visited

max_url = '' #url with most out links
max_url_count = 0 #number of out links ^
invalid_links = 0 #number of invalid links from the frontier

try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

@Producer(ProducedLink, Link)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "13353358_34811375_90269441"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR S17 UnderGrad 13353358, 34811375, 90269441"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################

'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''

def save_to_file():
    with open('analytics.txt', 'w') as file:
        for key, value in subdomaincount.iteritems():
            file.write("{}: {}\n".format(key, value))
        file.write("MOST OUT LINKS: " + max_url + "\nOUT LINKS: " + str(max_url_count) + "\n")
        file.write("INVALID URLS FROM THE FRONTIER: " + str(invalid_links))

def extract_next_links(rawDatas):

    count = 0 #number of links from this rawData url

    outputLinks = list()

    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''

    for i in rawDatas:

        # if (not is_valid(i.url)):
        #     global invalid_links
        #     invalid_links += 1
        #     print("INVALID URL")
        #     return
        
        #Extract domain from URL
        domain = '{uri.scheme}://{uri.netloc}'.format(uri=urlparse(i.url)) 

        #Checks if raw object has error message, if so, skips scraping its content
        if not i.error_message:
            print("ERROR MESSAGE") #FOR TESTING ONLY
            # i.bad_url = True
            # return

            try:                
                parser = etree.HTMLParser()
                tree = etree.parse(StringIO(i.content.decode('utf-8')), parser)
                urls = tree.xpath("//@href") #/a[not(contains(@href, '.php'))]
            except:
                print("ERROR PARSING")

            for url in urls: #checking urls that is scraped from the rawdata url

                if scraped_url_is_valid(url): #This can be done after checking if URL is absolute. Please verify - Andres
                    print(url) #FOR TESTING ONLY
                    count += 1 #Are you keeping track of valid or invalid URLs?? - Andres

                #Check if URL is absolute
                if not absolute_form(url):
                    if url[0] == '/':
                        absoluteURL = relative_to_absolute_url(domain, url)
                    else:
                        absoluteURL = relative_to_absolute_url(i.url, url)

                    #outputLinks.append(absoluteURL) #UNCOMMENT WHEN READY
                else:
                    #outputLinks.append(url) #UNCOMMENT WHEN READY
            
            if max_url_count < count:
                # if this url has more valid urls than the current max count url
                global max_url_count
                global max_url
                max_url = i.url
                max_url_count = count

    save_to_file()
       
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        state = ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*mailto:.*|.*(/misc|/policies|/degrees|/sao|/computing|sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){2}.*|.*calendar.*|.*\.(php\?|css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        if not state: #Keep track of invalid links encountered
            global invalid_links
            invalid_links += 1
            print "\nTHAT WAS AN INVALID LINK\n"

        else: #Keep track of subdomains visited and the number of different urls processed from each subdomain
            split_url = url.split('.')
            subdomain = split_url[1]
            global subdomaincount
            subdomaincount[subdomain] = subdomaincount.get(subdomain, 0) + 1
            
        return state


    except TypeError:
        print ("TypeError for ", parsed)

def scraped_url_is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        state = ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*mailto:.*|.*(/misc|/policies|/degrees|/sao|/computing|sites|/all|/themes|/modules|/profiles|/css|/field|/node|/theme){2}.*|.*calendar.*|.*\.(php\?|css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
        return state

    except TypeError:
        print ("TypeError for ", parsed)

def absolute_form(url):
    '''
    Return True if url is in absolute form
    otherwise False
    '''
    parsed = urlparsed(url)

    if parsed.scheme not in set(["http", "https"]):
        return False
    return True

def relative_to_absolute_url(domain, relativeURL):
    '''
    Function returns the absolute form of a URL given the domain and relative URL

    Domain Example: 'http://www.ics.uci.edu'
    relativeURL Example: '/about/equity/'
    output: 'http://www.ics.uci.edu/about/equity/' 

    '''
    return domain + relativeURL

