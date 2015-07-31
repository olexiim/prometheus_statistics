# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 23:30:16 2015

@author: oleksii
"""

import MySQLdb
import time
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
    result["Number of registered users"] = (data[0], "{0:,d}".format(data[0]))
    users_amount = data[0]
    
    # Number of certificates
    cursor = db.cursor()
    sql = "SELECT COUNT( 1 ) FROM certificates_generatedcertificate " \
            "WHERE course_id LIKE '%s' AND status LIKE 'downloadable'" % (course_title)
    cursor.execute(sql)
    data = cursor.fetchone()
    percent = 100*float(data[0]) / users_amount
    result["Number of certificates"] = (data[0], "{0:,d} ({1:.2f}%)".format(data[0],percent))
    
    # Age groups and education level
    cursor = db.cursor()
    sql = "SELECT b.year_of_birth AS YEAR, b.gender AS gender, b.level_of_education AS level " \
            "FROM student_courseenrollment a " \
            "LEFT JOIN auth_userprofile b ON a.user_id = b.user_id " \
            "WHERE a.course_id = '%s'" % (course_title)
    cursor.execute(sql)
    data = cursor.fetchall()
    
    ages = {'NA':0, '<20':0, '20-24':0, '25-29':0, '30-34':0, '35-39':0, '40-44': 0, '45-49':0, '>=50':0}
    ages_list = ('NA', '<20', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', '>=50')
    Year = time.localtime().tm_year
    s, cntr = 0, 0
    m, f = 0, 0
    edlevel = 0

    for line in data:
        if line[0]==None:
            ages['NA'] += 1
        else:
            age = Year - int(line[0])
            s += age
            cntr += 1
            if age<20:
                ages['<20'] += 1
            elif age<25:
                ages['20-24'] += 1
            elif age<30:
                ages['25-29'] += 1
            elif age<35:
                ages['30-34'] += 1
            elif age<40:
                ages['35-39'] += 1
            elif age<45:
                ages['40-44'] += 1
            elif age<50:
                ages['45-49'] += 1
            else:
                ages['>=50'] += 1
                
            if line[1]=='m':
                m += 1
            elif line[1]=='f':
                f += 1
                
            if line[2] in ['p','m','b','a']:
                edlevel += 1
                
    result["User age groups"] = (ages, '\n'.join(["{0}:\t{1:,d}\t({2:.2f}%)".format(age,ages[age],100*float(ages[age])/users_amount) for age in ages_list]))
    result["Age medium"] = (s//cntr, "{0} years".format(s//cntr))
    result["Female user percentage"] = (f, "{0:.2f}%".format(float(f)*100/users_amount))
    result["Bachelor and higher educated users (percents)"] = (edlevel, "{0:.2f}%".format(float(edlevel)*100/users_amount))
    
    return result
    
def write_course_data_detailed(course_title, data, output_file):
    with open(output_file,'w') as f:
        f.write("Course: {0}\n\n".format(course_title))
        i = 1
        for key,value in data.iteritems():
            f.write("{idx}. {title}\n{value}\n\n".format(idx=i, title=key, value=value[1]))
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
            write_course_data_detailed(course_title, course_data, output_file)
