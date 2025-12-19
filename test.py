import xlsxwriter


work = xlsxwriter.Workbook()

form = work.add_format({'border': 5, 'align': 'center'})
form2 = work.add_format({'border': 1})

print(work.formats)