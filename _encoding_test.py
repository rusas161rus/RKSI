from docx import Document

d = Document()
d.add_paragraph('Проверка кириллицы: Дипломный проект, Расписание занятий, РКСИ')
d.save(r'E:\codex_progects\RKSI_ubuntu\_encoding_test.docx')
print('ok')
