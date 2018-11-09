#
# Query to get stub metadata before optimizer fixup
# uses: dumpPages ( $this->history & self::FULL )
#
EXPLAIN EXTENDED SELECT
rev_id,rev_page,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,
COALESCE( comment_rev_comment.comment_text, rev_comment ),comment_data,comment_id,
rev_user,rev_user_text,
NULL AS `rev_actor`,
rev_text_id,rev_content_format,rev_content_model,
page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,page_restrictions,
rev_text_id
FROM page
INNER JOIN revision ON ((page_id = rev_page))
LEFT JOIN `revision_comment_temp` `temp_rev_comment` ON ((revcomment_rev = rev_id))
LEFT JOIN `comment` `comment_rev_comment` ON ((comment_id = revcomment_comment_id))
WHERE (page_id >= $STARTPAGE AND page_id < $ENDPAGE) AND (rev_page>$BIGPAGE OR (rev_page=$BIGPAGE AND rev_id>$REVID))
ORDER BY rev_page ASC,rev_id ASC LIMIT 50000;
----------------------
#
# Query to get stub metadata after optimizer fixup, no offset
# uses: dumpPages ( $this->history & self::FULL )
#
EXPLAIN EXTENDED SELECT
/*! STRAIGHT_JOIN */
rev_id,rev_page,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,
COALESCE( comment_rev_comment.comment_text, rev_comment ),comment_data,comment_id,
rev_user,rev_user_text,
NULL AS `rev_actor`,
rev_text_id,rev_content_format,rev_content_model,
page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,page_restrictions,
rev_text_id  FROM `revision`
FORCE INDEX (rev_page_id)
LEFT JOIN `revision_comment_temp` `temp_rev_comment` ON ((revcomment_rev = rev_id))
LEFT JOIN `comment` `comment_rev_comment` ON ((comment_id = revcomment_comment_id))
INNER JOIN `page` ON ((rev_page=page_id))
WHERE (page_id >= $STARTPAGE AND page_id < $ENDPAGE) AND (rev_page>0 OR (rev_page=0 AND rev_id>0))
ORDER BY rev_page ASC,rev_id ASC LIMIT 50000;
----------------------
#
# Query to get stub metadata after optimizer fixup, offset
# uses: dumpPages ( $this->history & self::FULL )
#
EXPLAIN EXTENDED SELECT
/*! STRAIGHT_JOIN */
rev_id,rev_page,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,
COALESCE( comment_rev_comment.comment_text, rev_comment ),comment_data,comment_id,
rev_user,rev_user_text,
NULL AS `rev_actor`,
rev_text_id,rev_content_format,rev_content_model,
page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,page_restrictions,
rev_text_id  FROM `revision`
FORCE INDEX (rev_page_id)
LEFT JOIN `revision_comment_temp` `temp_rev_comment` ON ((revcomment_rev = rev_id))
LEFT JOIN `comment` `comment_rev_comment` ON ((comment_id = revcomment_comment_id))
INNER JOIN `page` ON ((rev_page=page_id))
WHERE (page_id >= $STARTPAGE AND page_id < $ENDPAGE) AND (rev_page>$BIGPAGE OR (rev_page=$BIGPAGE AND rev_id>$REVID))
ORDER BY rev_page ASC,rev_id ASC LIMIT 50000;
----------------------
#
# Query for Special:Export of several pages, no revision history; each page
# is requested separately
# uses: dumpPages ( $this->history & self::CURRENT )
#
EXPLAIN EXTENDED SELECT
rev_id,rev_page,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,
COALESCE( comment_rev_comment.comment_text, rev_comment ),comment_data,comment_id,
rev_user,rev_user_text,
NULL AS `rev_actor`,
page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,
old_text,old_flags,
page_restrictions,
rev_text_id
FROM `page`
INNER JOIN `revision` ON ((page_id=rev_page AND page_latest=rev_id))
LEFT JOIN `revision_comment_temp` `temp_rev_comment` ON ((revcomment_rev = rev_id))
LEFT JOIN `comment` `comment_rev_comment` ON ((comment_id = revcomment_comment_id))
INNER JOIN `text` ON ((rev_text_id=old_id))
WHERE (page_namespace=$NAMESPACE AND page_title='$TITLE') AND (rev_page>0 OR (rev_page=0 AND rev_id>0))
ORDER BY page_id ASC LIMIT 50000;
----------------------
#
# Query for Special:Export of several pages, no revision history; each page
# is requested separately
# starts from offset in middle of page with plenty of revisions
# uses: dumpPages ( $this->history & self::CURRENT )
#
EXPLAIN EXTENDED SELECT
rev_id,rev_page,rev_timestamp,rev_minor_edit,rev_deleted,rev_len,rev_parent_id,rev_sha1,
COALESCE( comment_rev_comment.comment_text, rev_comment ),comment_data,comment_id,
rev_user,rev_user_text,
NULL AS `rev_actor`,
page_namespace,page_title,page_id,page_latest,page_is_redirect,page_len,
old_text,old_flags,
page_restrictions,
rev_text_id
FROM `page`
INNER JOIN `revision` ON ((page_id=rev_page AND page_latest=rev_id))
LEFT JOIN `revision_comment_temp` `temp_rev_comment` ON ((revcomment_rev = rev_id))
LEFT JOIN `comment` `comment_rev_comment` ON ((comment_id = revcomment_comment_id))
INNER JOIN `text` ON ((rev_text_id=old_id))
WHERE (page_namespace=$NAMESPACE AND page_title='$TITLE') AND (rev_page>$BIGPAGE OR (rev_page=$BIGPAGE AND rev_id>$REVID))
ORDER BY page_id ASC LIMIT 50000;
