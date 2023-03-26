select bitmapToArray(
        bitmapAnd (
            (
                select groupBitmapMergeState(us)
                from user_tag_value_string
                where tag_value in ('90后', '80后')
                    and tag_code = 'agegroup'
            ),
            (
                select groupBitmapMergeState(us)
                from user_tag_value_string
                where tag_value in ('ms', 'sj')
                    and tag_code = 'favor'
            )
        )
    );