SELECT page_num,
       flags,
       raw_chars,
       corrected_chars,
       ROUND(length_ratio, 2) AS ratio,
       raw_text,
       corrected_text
FROM pages
ORDER BY source_file, page_num;