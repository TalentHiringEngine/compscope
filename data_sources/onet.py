"""
O*NET Client — Job Title to SOC Code Matching
==============================================
Maps free-text job titles to SOC codes using a curated local map
first (instant, no API), then fuzzy match, then O*NET API if
credentials are configured.

API docs: https://services.onetcenter.org/
Set ONET_USERNAME / ONET_PASSWORD in Streamlit secrets.
"""

import os
import requests
import re
from difflib import SequenceMatcher


ONET_BASE = "https://services.onetcenter.org/ws/"


# ── Curated title → SOC map ───────────────────────────────────────────────────
# All keys are lowercase normalized. Add liberally — this is the fastest path.
LOCAL_TITLE_MAP = {

    # ── Software Engineering ──────────────────────────────────────────────────
    "software engineer":               ("15-1252.00", "Software Developers"),
    "software developer":              ("15-1252.00", "Software Developers"),
    "swe":                             ("15-1252.00", "Software Developers"),
    "junior software engineer":        ("15-1252.00", "Software Developers"),
    "senior software engineer":        ("15-1252.00", "Software Developers"),
    "sr software engineer":            ("15-1252.00", "Software Developers"),
    "sr. software engineer":           ("15-1252.00", "Software Developers"),
    "staff software engineer":         ("15-1252.00", "Software Developers"),
    "principal software engineer":     ("15-1252.00", "Software Developers"),
    "lead software engineer":          ("15-1252.00", "Software Developers"),
    "software engineer ii":            ("15-1252.00", "Software Developers"),
    "software engineer iii":           ("15-1252.00", "Software Developers"),
    "frontend engineer":               ("15-1252.00", "Software Developers"),
    "front end engineer":              ("15-1252.00", "Software Developers"),
    "front-end engineer":              ("15-1252.00", "Software Developers"),
    "backend engineer":                ("15-1252.00", "Software Developers"),
    "back end engineer":               ("15-1252.00", "Software Developers"),
    "back-end engineer":               ("15-1252.00", "Software Developers"),
    "fullstack engineer":              ("15-1252.00", "Software Developers"),
    "full stack engineer":             ("15-1252.00", "Software Developers"),
    "full-stack engineer":             ("15-1252.00", "Software Developers"),
    "frontend developer":              ("15-1252.00", "Software Developers"),
    "backend developer":               ("15-1252.00", "Software Developers"),
    "web developer":                   ("15-1252.00", "Software Developers"),
    "web engineer":                    ("15-1252.00", "Software Developers"),
    "application developer":           ("15-1252.00", "Software Developers"),
    "application engineer":            ("15-1252.00", "Software Developers"),

    # ── Data Engineering ──────────────────────────────────────────────────────
    "data engineer":                   ("15-1243.00", "Database Architects"),
    "junior data engineer":            ("15-1243.00", "Database Architects"),
    "senior data engineer":            ("15-1243.00", "Database Architects"),
    "sr data engineer":                ("15-1243.00", "Database Architects"),
    "sr. data engineer":               ("15-1243.00", "Database Architects"),
    "lead data engineer":              ("15-1243.00", "Database Architects"),
    "staff data engineer":             ("15-1243.00", "Database Architects"),
    "principal data engineer":         ("15-1243.00", "Database Architects"),
    "data engineer ii":                ("15-1243.00", "Database Architects"),
    "data engineer iii":               ("15-1243.00", "Database Architects"),
    "database engineer":               ("15-1243.00", "Database Architects"),
    "database architect":              ("15-1243.00", "Database Architects"),
    "analytics engineer":              ("15-1243.00", "Database Architects"),
    "data platform engineer":          ("15-1243.00", "Database Architects"),
    "data infrastructure engineer":    ("15-1243.00", "Database Architects"),

    # ── Data Science / ML ─────────────────────────────────────────────────────
    "data scientist":                  ("15-2051.00", "Data Scientists"),
    "senior data scientist":           ("15-2051.00", "Data Scientists"),
    "sr data scientist":               ("15-2051.00", "Data Scientists"),
    "lead data scientist":             ("15-2051.00", "Data Scientists"),
    "staff data scientist":            ("15-2051.00", "Data Scientists"),
    "principal data scientist":        ("15-2051.00", "Data Scientists"),
    "machine learning engineer":       ("15-2051.01", "Data Scientists, All Other"),
    "ml engineer":                     ("15-2051.01", "Data Scientists, All Other"),
    "senior machine learning engineer":("15-2051.01", "Data Scientists, All Other"),
    "sr ml engineer":                  ("15-2051.01", "Data Scientists, All Other"),
    "ai engineer":                     ("15-2051.01", "Data Scientists, All Other"),
    "applied scientist":               ("15-2051.00", "Data Scientists"),
    "research scientist":              ("15-2051.00", "Data Scientists"),

    # ── Data Analysis ─────────────────────────────────────────────────────────
    "data analyst":                    ("15-2041.00", "Statisticians"),
    "senior data analyst":             ("15-2041.00", "Statisticians"),
    "sr data analyst":                 ("15-2041.00", "Statisticians"),
    "business intelligence analyst":   ("15-2041.00", "Statisticians"),
    "bi analyst":                      ("15-2041.00", "Statisticians"),
    "business intelligence engineer":  ("15-2041.00", "Statisticians"),
    "bi engineer":                     ("15-2041.00", "Statisticians"),

    # ── Infrastructure / DevOps ───────────────────────────────────────────────
    "devops engineer":                 ("15-1244.00", "Network and Computer Systems Administrators"),
    "senior devops engineer":          ("15-1244.00", "Network and Computer Systems Administrators"),
    "sr devops engineer":              ("15-1244.00", "Network and Computer Systems Administrators"),
    "site reliability engineer":       ("15-1244.00", "Network and Computer Systems Administrators"),
    "sre":                             ("15-1244.00", "Network and Computer Systems Administrators"),
    "platform engineer":               ("15-1244.00", "Network and Computer Systems Administrators"),
    "infrastructure engineer":         ("15-1244.00", "Network and Computer Systems Administrators"),
    "systems administrator":           ("15-1244.00", "Network and Computer Systems Administrators"),
    "sysadmin":                        ("15-1244.00", "Network and Computer Systems Administrators"),
    "cloud engineer":                  ("15-1241.00", "Computer Network Architects"),
    "cloud architect":                 ("15-1241.00", "Computer Network Architects"),
    "solutions architect":             ("15-1241.00", "Computer Network Architects"),
    "enterprise architect":            ("15-1241.00", "Computer Network Architects"),
    "network engineer":                ("15-1241.00", "Computer Network Architects"),
    "network administrator":           ("15-1244.00", "Network and Computer Systems Administrators"),

    # ── Security ──────────────────────────────────────────────────────────────
    "cybersecurity analyst":           ("15-1212.00", "Information Security Analysts"),
    "cybersecurity engineer":          ("15-1212.00", "Information Security Analysts"),
    "security analyst":                ("15-1212.00", "Information Security Analysts"),
    "information security analyst":    ("15-1212.00", "Information Security Analysts"),
    "penetration tester":              ("15-1212.00", "Information Security Analysts"),
    "security engineer":               ("15-1212.00", "Information Security Analysts"),

    # ── IT / Support ──────────────────────────────────────────────────────────
    "it support specialist":           ("15-1232.00", "Computer User Support Specialists"),
    "help desk technician":            ("15-1232.00", "Computer User Support Specialists"),
    "desktop support":                 ("15-1232.00", "Computer User Support Specialists"),
    "it analyst":                      ("15-1299.08", "Computer Systems Analysts"),
    "systems analyst":                 ("15-1299.08", "Computer Systems Analysts"),
    "business systems analyst":        ("15-1299.08", "Computer Systems Analysts"),

    # ── Engineering Management ────────────────────────────────────────────────
    "engineering manager":             ("11-9041.00", "Architectural and Engineering Managers"),
    "senior engineering manager":      ("11-9041.00", "Architectural and Engineering Managers"),
    "director of engineering":         ("11-9041.00", "Architectural and Engineering Managers"),
    "vp of engineering":               ("11-9041.00", "Architectural and Engineering Managers"),
    "vp engineering":                  ("11-9041.00", "Architectural and Engineering Managers"),
    "head of engineering":             ("11-9041.00", "Architectural and Engineering Managers"),
    "cto":                             ("11-1021.00", "General and Operations Managers"),
    "chief technology officer":        ("11-1021.00", "General and Operations Managers"),

    # ── Project / Program Management ─────────────────────────────────────────
    "technical program manager":       ("15-1299.09", "Information Technology Project Managers"),
    "tpm":                             ("15-1299.09", "Information Technology Project Managers"),
    "it project manager":              ("15-1299.09", "Information Technology Project Managers"),
    "project manager":                 ("11-9199.00", "Managers, All Other"),
    "senior project manager":          ("11-9199.00", "Managers, All Other"),
    "program manager":                 ("11-9199.00", "Managers, All Other"),
    "senior program manager":          ("11-9199.00", "Managers, All Other"),
    "scrum master":                    ("15-1299.09", "Information Technology Project Managers"),
    "agile coach":                     ("15-1299.09", "Information Technology Project Managers"),

    # ── Product Management ────────────────────────────────────────────────────
    "product manager":                 ("11-2021.00", "Marketing Managers"),
    "senior product manager":          ("11-2021.00", "Marketing Managers"),
    "sr product manager":              ("11-2021.00", "Marketing Managers"),
    "product manager (tech)":          ("15-1299.09", "Information Technology Project Managers"),
    "director of product":             ("11-2021.00", "Marketing Managers"),
    "vp of product":                   ("11-2021.00", "Marketing Managers"),
    "vp product":                      ("11-2021.00", "Marketing Managers"),
    "chief product officer":           ("11-2021.00", "Marketing Managers"),
    "cpo":                             ("11-2021.00", "Marketing Managers"),

    # ── Design ────────────────────────────────────────────────────────────────
    "ux designer":                     ("27-1021.00", "Commercial and Industrial Designers"),
    "ui designer":                     ("27-1021.00", "Commercial and Industrial Designers"),
    "ux/ui designer":                  ("27-1021.00", "Commercial and Industrial Designers"),
    "product designer":                ("27-1021.00", "Commercial and Industrial Designers"),
    "user experience designer":        ("27-1021.00", "Commercial and Industrial Designers"),
    "graphic designer":                ("27-1024.00", "Graphic Designers"),
    "visual designer":                 ("27-1024.00", "Graphic Designers"),

    # ── Healthcare ────────────────────────────────────────────────────────────
    "registered nurse":                ("29-1141.00", "Registered Nurses"),
    "rn":                              ("29-1141.00", "Registered Nurses"),
    "staff nurse":                     ("29-1141.00", "Registered Nurses"),
    "nurse practitioner":              ("29-1171.00", "Nurse Practitioners"),
    "np":                              ("29-1171.00", "Nurse Practitioners"),
    "physician":                       ("29-1215.00", "Family Medicine Physicians"),
    "doctor":                          ("29-1215.00", "Family Medicine Physicians"),
    "md":                              ("29-1215.00", "Family Medicine Physicians"),
    "physician assistant":             ("29-1071.00", "Physician Assistants"),
    "pa":                              ("29-1071.00", "Physician Assistants"),
    "physical therapist":              ("29-1123.00", "Physical Therapists"),
    "pt":                              ("29-1123.00", "Physical Therapists"),
    "occupational therapist":          ("29-1122.00", "Occupational Therapists"),
    "ot":                              ("29-1122.00", "Occupational Therapists"),
    "medical assistant":               ("31-9092.00", "Medical Assistants"),
    "cna":                             ("31-1131.00", "Nursing Assistants"),
    "certified nursing assistant":     ("31-1131.00", "Nursing Assistants"),
    "pharmacist":                      ("29-1051.00", "Pharmacists"),

    # ── Finance / Accounting ──────────────────────────────────────────────────
    "accountant":                      ("13-2011.00", "Accountants and Auditors"),
    "senior accountant":               ("13-2011.00", "Accountants and Auditors"),
    "cpa":                             ("13-2011.00", "Accountants and Auditors"),
    "staff accountant":                ("13-2011.00", "Accountants and Auditors"),
    "controller":                      ("13-2011.00", "Accountants and Auditors"),
    "financial analyst":               ("13-2051.00", "Financial and Investment Analysts"),
    "senior financial analyst":        ("13-2051.00", "Financial and Investment Analysts"),
    "sr financial analyst":            ("13-2051.00", "Financial and Investment Analysts"),
    "fp&a analyst":                    ("13-2051.00", "Financial and Investment Analysts"),
    "fp&a manager":                    ("13-2051.00", "Financial and Investment Analysts"),
    "investment analyst":              ("13-2051.00", "Financial and Investment Analysts"),
    "finance manager":                 ("13-2051.00", "Financial and Investment Analysts"),
    "cfo":                             ("11-3031.00", "Financial Managers"),
    "chief financial officer":         ("11-3031.00", "Financial Managers"),
    "financial manager":               ("11-3031.00", "Financial Managers"),
    "treasury analyst":                ("13-2051.00", "Financial and Investment Analysts"),

    # ── Business / Operations ─────────────────────────────────────────────────
    "business analyst":                ("13-1111.00", "Management Analysts"),
    "senior business analyst":         ("13-1111.00", "Management Analysts"),
    "sr business analyst":             ("13-1111.00", "Management Analysts"),
    "management consultant":           ("13-1111.00", "Management Analysts"),
    "operations manager":              ("11-1021.00", "General and Operations Managers"),
    "operations analyst":              ("13-1111.00", "Management Analysts"),
    "strategy analyst":                ("13-1111.00", "Management Analysts"),
    "business operations manager":     ("11-1021.00", "General and Operations Managers"),
    "general manager":                 ("11-1021.00", "General and Operations Managers"),

    # ── Marketing ─────────────────────────────────────────────────────────────
    "marketing manager":               ("11-2021.00", "Marketing Managers"),
    "senior marketing manager":        ("11-2021.00", "Marketing Managers"),
    "marketing director":              ("11-2021.00", "Marketing Managers"),
    "marketing analyst":               ("13-1161.00", "Market Research Analysts"),
    "market research analyst":         ("13-1161.00", "Market Research Analysts"),
    "digital marketing manager":       ("11-2021.00", "Marketing Managers"),
    "content manager":                 ("11-2021.00", "Marketing Managers"),
    "seo specialist":                  ("13-1161.00", "Market Research Analysts"),
    "cmo":                             ("11-2021.00", "Marketing Managers"),
    "chief marketing officer":         ("11-2021.00", "Marketing Managers"),

    # ── Sales ─────────────────────────────────────────────────────────────────
    "sales representative":            ("41-4012.00", "Sales Representatives"),
    "sales rep":                       ("41-4012.00", "Sales Representatives"),
    "account executive":               ("41-4012.00", "Sales Representatives"),
    "ae":                              ("41-4012.00", "Sales Representatives"),
    "account manager":                 ("41-4012.00", "Sales Representatives"),
    "inside sales":                    ("41-4012.00", "Sales Representatives"),
    "outside sales":                   ("41-4012.00", "Sales Representatives"),
    "sales manager":                   ("41-1012.00", "Sales Managers"),
    "senior sales manager":            ("41-1012.00", "Sales Managers"),
    "vp of sales":                     ("41-1012.00", "Sales Managers"),
    "vp sales":                        ("41-1012.00", "Sales Managers"),
    "director of sales":               ("41-1012.00", "Sales Managers"),
    "business development manager":    ("41-1012.00", "Sales Managers"),
    "bdr":                             ("41-4012.00", "Sales Representatives"),
    "sdr":                             ("41-4012.00", "Sales Representatives"),

    # ── Human Resources ───────────────────────────────────────────────────────
    "human resources":                 ("13-1071.00", "Human Resources Specialists"),
    "hr":                              ("13-1071.00", "Human Resources Specialists"),
    "human resources specialist":      ("13-1071.00", "Human Resources Specialists"),
    "hr specialist":                   ("13-1071.00", "Human Resources Specialists"),
    "hr generalist":                   ("13-1071.00", "Human Resources Specialists"),
    "human resources generalist":      ("13-1071.00", "Human Resources Specialists"),
    "human resources manager":         ("11-3121.00", "Human Resources Managers"),
    "hr manager":                      ("11-3121.00", "Human Resources Managers"),
    "senior hr manager":               ("11-3121.00", "Human Resources Managers"),
    "director of human resources":     ("11-3121.00", "Human Resources Managers"),
    "hr director":                     ("11-3121.00", "Human Resources Managers"),
    "vp of human resources":           ("11-3121.00", "Human Resources Managers"),
    "vp hr":                           ("11-3121.00", "Human Resources Managers"),
    "chief human resources officer":   ("11-3121.00", "Human Resources Managers"),
    "chro":                            ("11-3121.00", "Human Resources Managers"),
    "hrbp":                            ("13-1071.00", "Human Resources Specialists"),
    "hr business partner":             ("13-1071.00", "Human Resources Specialists"),
    "compensation analyst":            ("13-1141.00", "Compensation, Benefits, and Job Analysis Specialists"),
    "compensation manager":            ("11-3111.00", "Compensation and Benefits Managers"),
    "benefits specialist":             ("13-1141.00", "Compensation, Benefits, and Job Analysis Specialists"),
    "benefits manager":                ("11-3111.00", "Compensation and Benefits Managers"),
    "training specialist":             ("13-1151.00", "Training and Development Specialists"),
    "learning and development":        ("13-1151.00", "Training and Development Specialists"),
    "talent management":               ("13-1071.00", "Human Resources Specialists"),
    "recruiter":                       ("13-1071.00", "Human Resources Specialists"),
    "senior recruiter":                ("13-1071.00", "Human Resources Specialists"),
    "technical recruiter":             ("13-1071.00", "Human Resources Specialists"),
    "talent acquisition":              ("13-1071.00", "Human Resources Specialists"),
    "talent acquisition specialist":   ("13-1071.00", "Human Resources Specialists"),
    "talent acquisition manager":      ("11-3121.00", "Human Resources Managers"),
    "sourcer":                         ("13-1071.00", "Human Resources Specialists"),
    "staffing specialist":             ("13-1071.00", "Human Resources Specialists"),
    "staffing coordinator":            ("13-1071.00", "Human Resources Specialists"),
    "staffing manager":                ("11-3121.00", "Human Resources Managers"),

    # ── Legal ─────────────────────────────────────────────────────────────────
    "attorney":                        ("23-1011.00", "Lawyers"),
    "lawyer":                          ("23-1011.00", "Lawyers"),
    "associate attorney":              ("23-1011.00", "Lawyers"),
    "paralegal":                       ("23-2011.00", "Paralegals and Legal Assistants"),
    "legal assistant":                 ("23-2011.00", "Paralegals and Legal Assistants"),
    "compliance analyst":              ("13-1041.00", "Compliance Officers"),
    "compliance officer":              ("13-1041.00", "Compliance Officers"),
    "compliance manager":              ("13-1041.00", "Compliance Officers"),

    # ── Education ─────────────────────────────────────────────────────────────
    "teacher":                         ("25-2031.00", "Secondary School Teachers"),
    "high school teacher":             ("25-2031.00", "Secondary School Teachers"),
    "elementary teacher":              ("25-2021.00", "Elementary School Teachers"),
    "professor":                       ("25-1099.00", "Postsecondary Teachers, All Other"),
    "adjunct professor":               ("25-1099.00", "Postsecondary Teachers, All Other"),
    "instructional designer":          ("25-9031.00", "Instructional Coordinators"),
    "curriculum developer":            ("25-9031.00", "Instructional Coordinators"),

    # ── Engineering (non-software) ────────────────────────────────────────────
    "mechanical engineer":             ("17-2141.00", "Mechanical Engineers"),
    "senior mechanical engineer":      ("17-2141.00", "Mechanical Engineers"),
    "electrical engineer":             ("17-2071.00", "Electrical Engineers"),
    "senior electrical engineer":      ("17-2071.00", "Electrical Engineers"),
    "civil engineer":                  ("17-2051.00", "Civil Engineers"),
    "structural engineer":             ("17-2051.00", "Civil Engineers"),
    "chemical engineer":               ("17-2041.00", "Chemical Engineers"),
    "aerospace engineer":              ("17-2011.00", "Aerospace Engineers"),
    "manufacturing engineer":          ("17-2112.00", "Industrial Engineers"),
    "industrial engineer":             ("17-2112.00", "Industrial Engineers"),
    "quality engineer":                ("17-2112.00", "Industrial Engineers"),
    "process engineer":                ("17-2112.00", "Industrial Engineers"),
    "plant engineer":                  ("17-2112.00", "Industrial Engineers"),

    # ── Manufacturing / Production ────────────────────────────────────────────
    "machine operator":                ("51-9199.00", "Production Workers, All Other"),
    "machine operator i":              ("51-9199.00", "Production Workers, All Other"),
    "machine operator ii":             ("51-9199.00", "Production Workers, All Other"),
    "production operator":             ("51-9199.00", "Production Workers, All Other"),
    "manufacturing operator":          ("51-9199.00", "Production Workers, All Other"),
    "line operator":                   ("51-9199.00", "Production Workers, All Other"),
    "production worker":               ("51-9199.00", "Production Workers, All Other"),
    "assembly operator":               ("51-2099.00", "Assemblers and Fabricators, All Other"),
    "assembler":                       ("51-2099.00", "Assemblers and Fabricators, All Other"),
    "production assembler":            ("51-2099.00", "Assemblers and Fabricators, All Other"),
    "cnc operator":                    ("51-4011.00", "CNC Machine Tool Operators, Metal and Plastic"),
    "cnc machine operator":            ("51-4011.00", "CNC Machine Tool Operators, Metal and Plastic"),
    "cnc machinist":                   ("51-4011.00", "CNC Machine Tool Operators, Metal and Plastic"),
    "machinist":                       ("51-4041.00", "Machinists"),
    "tool and die maker":              ("51-4111.00", "Tool and Die Makers"),
    "welder":                          ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "welding technician":              ("51-4121.00", "Welders, Cutters, Solderers, and Brazers"),
    "quality inspector":               ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "quality control inspector":       ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "qc inspector":                    ("51-9061.00", "Inspectors, Testers, Sorters, Samplers, and Weighers"),
    "production supervisor":           ("51-1011.00", "First-Line Supervisors of Production and Operating Workers"),
    "manufacturing supervisor":        ("51-1011.00", "First-Line Supervisors of Production and Operating Workers"),
    "shift supervisor":                ("51-1011.00", "First-Line Supervisors of Production and Operating Workers"),
    "plant manager":                   ("11-1021.00", "General and Operations Managers"),
    "production manager":              ("11-9199.00", "Managers, All Other"),

    # ── Warehouse / Logistics ─────────────────────────────────────────────────
    "warehouse worker":                ("53-7065.00", "Stockers and Order Fillers"),
    "warehouse associate":             ("53-7065.00", "Stockers and Order Fillers"),
    "warehouse operator":              ("53-7065.00", "Stockers and Order Fillers"),
    "order picker":                    ("53-7065.00", "Stockers and Order Fillers"),
    "picker packer":                   ("53-7065.00", "Stockers and Order Fillers"),
    "stocker":                         ("53-7065.00", "Stockers and Order Fillers"),
    "inventory specialist":            ("53-7065.00", "Stockers and Order Fillers"),
    "forklift operator":               ("53-7051.00", "Industrial Truck and Tractor Operators"),
    "forklift driver":                 ("53-7051.00", "Industrial Truck and Tractor Operators"),
    "material handler":                ("53-7062.00", "Laborers and Freight, Stock, and Material Movers, Hand"),
    "shipping receiving":              ("53-7065.00", "Stockers and Order Fillers"),
    "shipping coordinator":            ("43-5071.00", "Shipping, Receiving, and Inventory Clerks"),
    "logistics coordinator":           ("43-5071.00", "Shipping, Receiving, and Inventory Clerks"),
    "supply chain analyst":            ("13-1081.00", "Logisticians"),
    "supply chain manager":            ("11-3071.00", "Transportation, Storage, and Distribution Managers"),
    "logistics manager":               ("11-3071.00", "Transportation, Storage, and Distribution Managers"),

    # ── Transportation ────────────────────────────────────────────────────────
    "truck driver":                    ("53-3032.00", "Heavy and Tractor-Trailer Truck Drivers"),
    "cdl driver":                      ("53-3032.00", "Heavy and Tractor-Trailer Truck Drivers"),
    "semi driver":                     ("53-3032.00", "Heavy and Tractor-Trailer Truck Drivers"),
    "delivery driver":                 ("53-3033.00", "Light Truck Drivers"),
    "driver":                          ("53-3033.00", "Light Truck Drivers"),
    "bus driver":                      ("53-3052.00", "Bus Drivers, School"),
    "dispatcher":                      ("43-5032.00", "Dispatchers, Except Police, Fire, and Ambulance"),

    # ── Skilled Trades ────────────────────────────────────────────────────────
    "electrician":                     ("47-2111.00", "Electricians"),
    "journeyman electrician":          ("47-2111.00", "Electricians"),
    "master electrician":              ("47-2111.00", "Electricians"),
    "plumber":                         ("47-2152.00", "Plumbers, Pipefitters, and Steamfitters"),
    "pipefitter":                      ("47-2152.00", "Plumbers, Pipefitters, and Steamfitters"),
    "carpenter":                       ("47-2031.00", "Carpenters"),
    "construction worker":             ("47-2061.00", "Construction Laborers"),
    "laborer":                         ("47-2061.00", "Construction Laborers"),
    "hvac technician":                 ("49-9021.00", "Heating, Air Conditioning, and Refrigeration Mechanics and Installers"),
    "hvac tech":                       ("49-9021.00", "Heating, Air Conditioning, and Refrigeration Mechanics and Installers"),
    "hvac":                            ("49-9021.00", "Heating, Air Conditioning, and Refrigeration Mechanics and Installers"),
    "maintenance technician":          ("49-9071.00", "Maintenance and Repair Workers, General"),
    "maintenance mechanic":            ("49-9071.00", "Maintenance and Repair Workers, General"),
    "facilities technician":           ("49-9071.00", "Maintenance and Repair Workers, General"),
    "mechanic":                        ("49-3023.00", "Automotive Service Technicians and Mechanics"),
    "auto mechanic":                   ("49-3023.00", "Automotive Service Technicians and Mechanics"),
    "automotive technician":           ("49-3023.00", "Automotive Service Technicians and Mechanics"),
    "diesel mechanic":                 ("49-3031.00", "Bus and Truck Mechanics and Diesel Engine Specialists"),
    "equipment operator":              ("47-2073.00", "Operating Engineers and Other Construction Equipment Operators"),
    "heavy equipment operator":        ("47-2073.00", "Operating Engineers and Other Construction Equipment Operators"),

    # ── Customer Service / Retail ─────────────────────────────────────────────
    "customer service representative": ("43-4051.00", "Customer Service Representatives"),
    "customer service rep":            ("43-4051.00", "Customer Service Representatives"),
    "csr":                             ("43-4051.00", "Customer Service Representatives"),
    "call center agent":               ("43-4051.00", "Customer Service Representatives"),
    "customer support specialist":     ("43-4051.00", "Customer Service Representatives"),
    "retail associate":                ("41-2031.00", "Retail Salespersons"),
    "retail sales associate":          ("41-2031.00", "Retail Salespersons"),
    "cashier":                         ("41-2011.00", "Cashiers"),
    "store manager":                   ("41-1011.00", "First-Line Supervisors of Retail Sales Workers"),

    # ── Administrative ────────────────────────────────────────────────────────
    "administrative assistant":        ("43-6014.00", "Secretaries and Administrative Assistants"),
    "admin assistant":                 ("43-6014.00", "Secretaries and Administrative Assistants"),
    "executive assistant":             ("43-6011.00", "Executive Secretaries and Executive Administrative Assistants"),
    "office manager":                  ("43-1011.00", "First-Line Supervisors of Office and Administrative Support Workers"),
    "receptionist":                    ("43-4171.00", "Receptionists and Information Clerks"),
    "data entry":                      ("43-9021.00", "Data Entry Keyers"),
    "data entry clerk":                ("43-9021.00", "Data Entry Keyers"),
    "bookkeeper":                      ("43-3031.00", "Bookkeeping, Accounting, and Auditing Clerks"),
    "payroll specialist":              ("43-3051.00", "Payroll and Timekeeping Clerks"),
    "payroll manager":                 ("11-3111.00", "Compensation and Benefits Managers"),
}


def _normalize(title: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation variants."""
    return re.sub(r"\s+", " ", title.lower().strip())


class ONETClient:
    """
    Maps free-text job titles to O*NET SOC codes.

    Priority:
    1. Exact local map lookup (instant)
    2. Fuzzy match against local map (fast, threshold 0.68)
    3. O*NET keyword search API (if ONET_USERNAME is configured)
    """

    def __init__(self, username: str = None, password: str = None):
        self.username = username or os.getenv("ONET_USERNAME", "")
        self.password = password or os.getenv("ONET_PASSWORD", "")
        self.session  = requests.Session()
        if self.username:
            self.session.auth = (self.username, self.password)
        self.session.headers.update({"Accept": "application/json"})

    def search_occupations(self, title: str, max_results: int = 5) -> list[dict]:
        """
        Return up to max_results SOC matches, ranked by confidence.
        Each result: {"code": str, "title": str, "score": float, "source": str}
        """
        normalized = _normalize(title)
        results = []

        # 1. Exact local match
        if normalized in LOCAL_TITLE_MAP:
            code, onet_title = LOCAL_TITLE_MAP[normalized]
            results.append({
                "code":   code,
                "title":  onet_title,
                "score":  1.0,
                "source": "local_map",
            })

        # 2. Fuzzy match — threshold 0.68 to avoid false positives
        scored = []
        for key, (code, onet_title) in LOCAL_TITLE_MAP.items():
            ratio = SequenceMatcher(None, normalized, key).ratio()
            if ratio >= 0.68:
                scored.append((ratio, code, onet_title, key))

        scored.sort(reverse=True)
        seen_codes = {r["code"] for r in results}
        for ratio, code, onet_title, matched_key in scored[:max_results]:
            if code not in seen_codes:
                results.append({
                    "code":   code,
                    "title":  onet_title,
                    "score":  round(ratio, 3),
                    "source": f"fuzzy→{matched_key}",
                })
                seen_codes.add(code)

        # 3. O*NET API (only if credentials configured and still need more)
        if len(results) < 2 and self.username:
            for r in self._api_search(title):
                if r["code"] not in seen_codes:
                    results.append(r)
                    seen_codes.add(r["code"])

        return results[:max_results]

    def _api_search(self, title: str) -> list[dict]:
        """Authenticated O*NET keyword search."""
        try:
            resp = self.session.get(
                f"{ONET_BASE}search",
                params={"keyword": title, "start": 1, "end": 10, "fmt": "json"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                {
                    "code":   occ.get("code", ""),
                    "title":  occ.get("title", ""),
                    "score":  occ.get("relevance_score", 0.5),
                    "source": "onet_api",
                }
                for occ in data.get("occupation", [])
            ]
        except Exception as e:
            print(f"[O*NET] API search error: {e}")
            return []
