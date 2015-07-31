# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 23:30:16 2015

@author: oleksii
"""

import MySQLdb
from optparse import OptionParser

db_url = "localhost"
db_user = ""
db_pass = ""
db_name = ""
output_file = ''
db = None

headers = ["Number of registered users",
           "Number of certificates",
           "Number of users that watched first video of every week",
           "Number of user that recieved non-zero grade for test",
           "Number of user that started to pass the test",
           "User age groups",
           "Percentage of certificates for age groups",
           "Age medium",
           "Female user percentage",
           "Female cerified users",
           "Percentage of forum active users",
           "Number of forum message per one user",
           "Number of forum message per one active user",
           "Bachelor and higher educated users (percents)"]

#def write_output_header(output_file):
    

def get_course_data(course_title):
    result = {}

    # Number of registered users
    cursor = db.cursor()
    sql = "SELECT COUNT( 1 ) FROM student_courseenrollment WHERE course_id LIKE '%s'" % (course_title)
    cursor.execute(sql)
    data = cursor.fetchone()
    result["Number of registered users"] = data
    
    return result
    
def write_course_data_detailed(data, output_file):
    with open(output_file,'w') as f:
        i = 1
        for key,value in data.iteritems():
            f.write("{idx}. {title}\n{value}\n\n".format(idx=i, title=key, value=value))
            i += 1

if __name__=="__main__":
    usage = "usage: %prog [options] coursename"
    parser = OptionParser(usage=usage)
    parser.add_option("-r", "--db-url", dest="db_url", help="database URL", 
                      metavar="DB_URL", default=db_url)
    parser.add_option("-u", "--db-user", dest="db_user", help="database user", 
                      metavar="DB_USER", default=db_user)
    parser.add_option("-p", "--db-pass", dest="db_pass", help="database password", 
                      metavar="DB_PASS", default=db_pass)
    parser.add_option("-d", "--db-name", dest="db_name", help="database name", 
                      metavar="DB_NAME", default=db_name)
    parser.add_option("-o", "--output-file", dest="output_file", help="output file name", 
                      metavar="OUTPUT", default=output_file)
    parser.add_option("-a", "--output-append", dest="output_append", help="whether append data to existed file", 
                      metavar="OUTPUT_APPEND", action="store_true")
    parser.add_option("-f", "--output-format", dest="output_format", help="csv|detailed (default)", 
                      metavar="OUTPUT_FORMAT", default='detailed')                  
    
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")
    course = args[0]
                  
    db = MySQLdb.connect(options.db_url, options.db_user, options.db_pass, options.db_name )
    
    output_file = options.output_file
    
    if course=='all':
        courses = get_all_courses()
    else:
        courses = [course]
        
    for course_title in courses:
        course_data = get_course_data(course_title)
        
        if output_file=='':
            output_file = "{0}_output.txt".format(course_title.replace('/','_'))
        if options.output_format=='detailed':
            write_course_data_detailed(course_data, output_file)
