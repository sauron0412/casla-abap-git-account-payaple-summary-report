CLASS zcl_ap_summary_logic DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_rap_query_provider.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_AP_SUMMARY_LOGIC IMPLEMENTATION.


  METHOD if_rap_query_provider~select.

    TYPES: BEGIN OF lty_range_option,
             sign   TYPE c LENGTH 1,
             option TYPE c LENGTH 2,
             low    TYPE string,
             high   TYPE string,
           END OF lty_range_option.

    DATA: lt_result        TYPE TABLE OF zc_accpay_summary,
          lv_start_date    TYPE zc_accpay_summary-p_start_date,
          lv_end_date      TYPE zc_accpay_summary-p_end_date,
          lt_range         TYPE TABLE OF lty_range_option,
          lv_compcode_prov TYPE abap_bool,
          lv_bpgroup_prov  TYPE abap_bool,
          lv_account_prov  TYPE abap_bool,
          lv_partner_prov  TYPE abap_bool,
          lr_companycode   TYPE RANGE OF i_journalentryitem-companycode,
          lr_bpgroup       TYPE RANGE OF i_businesspartner-businesspartnergrouping,
          lr_account       TYPE RANGE OF i_journalentryitem-glaccount,
          lr_partner       TYPE RANGE OF i_journalentryitem-supplier,
          lv_currency      TYPE zc_accpay_summary-companycodecurrency.
    CLEAR: lt_result.

    " 1. Extract filter parameters
    CHECK io_request IS BOUND.
    TRY.
        DATA(lo_filter) = io_request->get_filter( ).
        CHECK lo_filter IS BOUND.
        DATA(lt_filter_ranges) = lo_filter->get_as_ranges( ).

        " Mandatory date filters
        READ TABLE lt_filter_ranges INTO DATA(ls_start_date) WITH KEY name = 'P_START_DATE'.
        IF sy-subrc = 0 AND ls_start_date-range IS NOT INITIAL.
          lv_start_date = ls_start_date-range[ 1 ]-low.
        ENDIF.

        READ TABLE lt_filter_ranges INTO DATA(ls_end_date) WITH KEY name = 'P_END_DATE'.
        IF sy-subrc = 0 AND ls_end_date-range IS NOT INITIAL.
          lv_end_date = ls_end_date-range[ 1 ]-low.
        ENDIF.
        "Mandatory currency filter
        READ TABLE lt_filter_ranges INTO DATA(ls_currency) WITH KEY name = 'RHCUR'.
        IF sy-subrc = 0 AND ls_currency-range IS NOT INITIAL.
          lv_currency = ls_currency-range[ 1 ]-low.
        ENDIF.

        " Optional filters with ALPHA conversion
        TRY.
            DATA(lr_compcode_raw) = lt_filter_ranges[ name = 'RBUKRS' ]-range.
            LOOP AT lr_compcode_raw ASSIGNING FIELD-SYMBOL(<fs_compcode>).
              IF <fs_compcode>-low IS NOT INITIAL.
                <fs_compcode>-low = |{ <fs_compcode>-low ALPHA = IN WIDTH = 4 }|.
              ENDIF.
              IF <fs_compcode>-high IS NOT INITIAL.
                <fs_compcode>-high = |{ <fs_compcode>-high ALPHA = IN WIDTH = 4 }|.
              ENDIF.
            ENDLOOP.
            MOVE-CORRESPONDING lr_compcode_raw TO lr_companycode.
            lv_compcode_prov = abap_true.
          CATCH cx_sy_itab_line_not_found.
            CLEAR lr_companycode.
        ENDTRY.

        TRY.
            DATA(lr_partner_raw) = lt_filter_ranges[ name = 'BP' ]-range.
            LOOP AT lr_partner_raw ASSIGNING FIELD-SYMBOL(<fs_partner>).
              IF <fs_partner>-low IS NOT INITIAL.
                <fs_partner>-low = |{ <fs_partner>-low ALPHA = IN WIDTH = 10 }|.
              ENDIF.
              IF <fs_partner>-high IS NOT INITIAL.
                <fs_partner>-high = |{ <fs_partner>-high ALPHA = IN WIDTH = 10 }|.
              ENDIF.
            ENDLOOP.
            MOVE-CORRESPONDING lr_partner_raw TO lr_partner.
            lv_partner_prov = abap_true.
          CATCH cx_sy_itab_line_not_found.
            CLEAR lr_partner.
        ENDTRY.

        TRY.
            DATA(lr_account_raw) = lt_filter_ranges[ name = 'ACCOUNTNUMBER' ]-range.
            LOOP AT lr_account_raw ASSIGNING FIELD-SYMBOL(<fs_account>).
              IF <fs_account>-low IS NOT INITIAL.
                <fs_account>-low = |{ <fs_account>-low ALPHA = IN WIDTH = 10 }|.
              ENDIF.
              IF <fs_account>-high IS NOT INITIAL.
                <fs_account>-high = |{ <fs_account>-high ALPHA = IN WIDTH = 10 }|.
              ENDIF.
            ENDLOOP.
            MOVE-CORRESPONDING lr_account_raw TO lr_account.
            lv_account_prov = abap_true.
          CATCH cx_sy_itab_line_not_found.
            CLEAR lr_account.
        ENDTRY.

        TRY.
            DATA(lr_bpgroup_raw) = lt_filter_ranges[ name = 'BP_GR' ]-range.
            LOOP AT lr_bpgroup_raw ASSIGNING FIELD-SYMBOL(<fs_bpgr>).
              IF <fs_bpgr>-low IS NOT INITIAL.
                <fs_bpgr>-low = |{ <fs_bpgr>-low ALPHA = IN WIDTH = 4 }|.
              ENDIF.
              IF <fs_bpgr>-high IS NOT INITIAL.
                <fs_bpgr>-high = |{ <fs_bpgr>-high ALPHA = IN WIDTH = 4 }|.
              ENDIF.
            ENDLOOP.
            MOVE-CORRESPONDING lr_bpgroup_raw TO lr_bpgroup.
            lv_bpgroup_prov = abap_true.
          CATCH cx_sy_itab_line_not_found.
            CLEAR lr_bpgroup.
        ENDTRY.

      CATCH cx_rap_query_filter_no_range INTO DATA(lx_filter_error).
        " Log error or raise message for debugging
        RETURN.
    ENDTRY.

    DATA: lt_where_clauses TYPE TABLE OF string.
    APPEND | postingdate >= @lv_start_date and postingdate <= @lv_end_date| TO lt_where_clauses.
    APPEND |and financialaccounttype = 'K'| TO lt_where_clauses.
    APPEND |and supplier IS NOT NULL| TO lt_where_clauses.
    APPEND |and debitcreditcode IN ('S', 'H')| TO lt_where_clauses.
    APPEND |and LEDGER = '0L'| TO lt_where_clauses.
    APPEND |and TRANSACTIONCURRENCY = @lv_currency| TO lt_where_clauses.

    IF lv_compcode_prov = abap_true.
      APPEND |and companycode IN @lr_companycode| TO lt_where_clauses.
    ENDIF.
    IF lv_partner_prov = abap_true.
      APPEND |and supplier IN @lr_partner| TO lt_where_clauses.
    ENDIF.
    IF lv_account_prov = abap_true.
      APPEND |and glaccount IN @lr_account| TO lt_where_clauses.
    ENDIF.

    " 2. Aggregate supplier data from I_JournalEntryItem
    " select total debit and credit amounts for each supplier, company code, currency, and GL account in period
    SELECT companycode AS rbukrs,
           supplier AS bp,
           transactioncurrency AS rhcur,
           glaccount AS accountnumber,
           companycodecurrency,
           SUM( CASE WHEN debitcreditcode = 'S' THEN amountincompanycodecurrency ELSE 0 END ) AS total_debit,
           SUM( CASE WHEN debitcreditcode = 'H' THEN amountincompanycodecurrency ELSE 0 END ) AS total_credit,
           SUM( CASE WHEN debitcreditcode = 'S' THEN amountintransactioncurrency ELSE 0 END ) AS total_debit_tran,
           SUM( CASE WHEN debitcreditcode = 'H' THEN amountintransactioncurrency ELSE 0 END ) AS total_credit_tran
      FROM i_journalentryitem
      WHERE (lt_where_clauses)
      GROUP BY companycode, supplier, companycodecurrency, glaccount, transactioncurrency
      INTO TABLE @DATA(lt_items).

    CHECK lt_items IS NOT INITIAL.
    CLEAR: lr_companycode, lr_partner.
    LOOP AT lt_items INTO DATA(ls_item1).
      " append company code and supplier to lr_companycode and lr_partner
      APPEND VALUE #( sign = 'I' option = 'EQ' low = ls_item1-rbukrs ) TO lr_companycode.
      APPEND VALUE #( sign = 'I' option = 'EQ' low = |{ ls_item1-bp ALPHA = IN WIDTH = 10 }| ) TO lr_partner.
    ENDLOOP.

    " 3. Fetch open and end balances in bulk
    SELECT supplier AS bp,
           companycode AS rbukrs,
           SUM( CASE WHEN debitcreditcode = 'S' THEN amountincompanycodecurrency ELSE 0 END ) AS open_debit,
           SUM( CASE WHEN debitcreditcode = 'H' THEN amountincompanycodecurrency ELSE 0 END ) AS open_credit,
           SUM( CASE WHEN debitcreditcode = 'S' THEN amountintransactioncurrency ELSE 0 END ) AS open_debit_tran,
           SUM( CASE WHEN debitcreditcode = 'H' THEN amountintransactioncurrency ELSE 0 END ) AS open_credit_tran
      FROM i_journalentryitem
      WHERE supplier IN @lr_partner
        AND postingdate < @lv_start_date
        AND companycode IN @lr_companycode
        AND ledger = '0L'
        AND financialaccounttype = 'K'
        AND supplier IS NOT NULL
        AND debitcreditcode IN ('S', 'H')
        AND glaccount IN @lr_account
*        AND ( clearingdate > @lv_start_date OR clearingdate IS NULL )
      GROUP BY supplier, companycode
      INTO TABLE @DATA(lt_open_balances).
    SORT lt_open_balances BY bp rbukrs.

*    SELECT supplier AS bp,
*           companycode AS rbukrs,
*           SUM( CASE WHEN debitcreditcode = 'S' THEN amountincompanycodecurrency ELSE 0 END ) AS end_debit,
*           SUM( CASE WHEN debitcreditcode = 'H' THEN amountincompanycodecurrency ELSE 0 END ) AS end_credit
*      FROM i_journalentryitem
*      WHERE supplier IN @lr_partner
*        AND companycode IN  @lr_companycode
*        AND postingdate <= @lv_end_date
*        AND ledger = '0L'
*        AND financialaccounttype = 'K'
*        AND supplier IS NOT NULL
*        AND debitcreditcode IN ('S', 'H')
**        AND ( clearingdate > @lv_end_date OR clearingdate IS NULL )
*      GROUP BY supplier, companycode
*      INTO TABLE @DATA(lt_end_balances).
*    SORT lt_end_balances BY bp rbukrs.

    " 4. Fetch supplier details
    SELECT supplier AS bp,
           suppliername AS bp_name
      FROM i_supplier
      WHERE supplier IN @lr_partner
      INTO TABLE @DATA(lt_suppliers).
    SORT lt_suppliers BY bp.

    SELECT b~businesspartner AS bp,
           b~businesspartnergrouping AS bp_gr,
           t~businesspartnergroupingtext AS bp_gr_title
      FROM i_businesspartner AS b
      LEFT OUTER JOIN i_businesspartnergroupingtext AS t
        ON t~businesspartnergrouping = b~businesspartnergrouping
        AND t~language = @sy-langu
      WHERE b~businesspartner IN @lr_partner
        AND b~businesspartnergrouping IN @lr_bpgroup
      INTO TABLE @DATA(lt_bp_groups).
    SORT lt_bp_groups BY bp.

    " 5. Build result table
    LOOP AT lt_items INTO DATA(ls_item).
      DATA(ls_result) = VALUE zc_accpay_summary(
        rbukrs = ls_item-rbukrs
        bp = ls_item-bp
        rhcur = ls_item-rhcur
        companycodecurrency = ls_item-companycodecurrency
        accountnumber = ls_item-accountnumber
        total_debit = ls_item-total_debit
        total_credit = ls_item-total_credit
        total_debit_tran = ls_item-total_debit_tran
        total_credit_tran = ls_item-total_credit_tran
        p_start_date = lv_start_date
        p_end_date = lv_end_date
      ).

      " Assign open balances
      READ TABLE lt_open_balances INTO DATA(ls_open) WITH KEY bp = ls_item-bp rbukrs = ls_item-rbukrs BINARY SEARCH.
      IF sy-subrc = 0.
        ls_result-open_debit = ls_open-open_debit.
        ls_result-open_credit = ls_open-open_credit.
        ls_result-open_debit_tran = ls_open-open_debit_tran.
        ls_result-open_credit_tran = ls_open-open_credit_tran.
      ENDIF.

      " Assign end balances
*      READ TABLE lt_end_balances INTO DATA(ls_end) WITH KEY bp = ls_item-bp rbukrs = ls_item-rbukrs BINARY SEARCH.
*      IF sy-subrc = 0.
*        ls_result-end_debit = ls_end-end_debit.
*        ls_result-end_credit = ls_end-end_credit.
*      ENDIF.


      " Assign supplier name
      READ TABLE lt_suppliers INTO DATA(ls_supplier) WITH KEY bp = ls_item-bp BINARY SEARCH.
      IF sy-subrc = 0.
        ls_result-bp_name = ls_supplier-bp_name.
      ENDIF.

      " Assign business partner group and title
      READ TABLE lt_bp_groups INTO DATA(ls_bp_group) WITH KEY bp = ls_item-bp BINARY SEARCH.
      IF sy-subrc = 0.
        ls_result-bp_gr = ls_bp_group-bp_gr.
        ls_result-bp_gr_title = ls_bp_group-bp_gr_title.
      ENDIF.

      APPEND ls_result TO lt_result.
      CLEAR ls_result.
    ENDLOOP.
    IF lv_bpgroup_prov = abap_true.
      DELETE lt_result WHERE bp_gr IS INITIAL.
    ENDIF.
    DATA: lv_open_amount TYPE zc_accpay_summary-open_debit,
          lv_end_amount  TYPE zc_accpay_summary-end_debit.
    LOOP AT lt_result ASSIGNING FIELD-SYMBOL(<fs_result>).
      lv_open_amount = <fs_result>-open_credit + <fs_result>-open_debit.
      IF lv_open_amount >= 0.
        CLEAR <fs_result>-open_credit.
        <fs_result>-open_debit = lv_open_amount.
      ELSE.
        CLEAR <fs_result>-open_debit.
        <fs_result>-open_credit = lv_open_amount.
      ENDIF.
      CLEAR lv_open_amount.
      lv_end_amount = <fs_result>-open_credit + <fs_result>-open_debit
                      + <fs_result>-total_credit + <fs_result>-total_debit.
      IF lv_end_amount >= 0.
        CLEAR <fs_result>-end_credit.
        <fs_result>-end_debit = lv_end_amount.
      ELSE.
        CLEAR <fs_result>-end_debit.
        <fs_result>-end_credit = lv_end_amount.
      ENDIF.
      CLEAR lv_end_amount.
      " transaction currency amounts
      lv_open_amount = <fs_result>-open_credit_tran + <fs_result>-open_debit_tran.
      IF lv_open_amount >= 0.
        CLEAR <fs_result>-open_credit_tran.
        <fs_result>-open_debit_tran = lv_open_amount.
      ELSE.
        CLEAR <fs_result>-open_debit_tran.
        <fs_result>-open_credit_tran = lv_open_amount.
      ENDIF.
      CLEAR lv_open_amount.
      lv_end_amount = <fs_result>-open_credit_tran + <fs_result>-open_debit_tran
                      + <fs_result>-total_credit_tran + <fs_result>-total_debit_tran.
      IF lv_end_amount >= 0.
        CLEAR <fs_result>-end_credit_tran.
        <fs_result>-end_debit_tran = lv_end_amount.
      ELSE.
        CLEAR <fs_result>-end_debit_tran.
        <fs_result>-end_credit_tran = lv_end_amount.
      ENDIF.
      CLEAR lv_end_amount.
    ENDLOOP.
    " 5. Change sign for all balance amounts
    LOOP AT lt_result ASSIGNING FIELD-SYMBOL(<lfs_temp>).
      <lfs_temp>-open_credit = abs( <lfs_temp>-open_credit ).
      <lfs_temp>-total_credit = <lfs_temp>-total_credit * -1.
      <lfs_temp>-end_credit = abs( <lfs_temp>-end_credit ).
      " Transaction currency amounts
      <lfs_temp>-open_credit_tran = abs( <lfs_temp>-open_credit_tran ).
      <lfs_temp>-total_credit_tran = <lfs_temp>-total_credit_tran * -1.
      <lfs_temp>-end_credit_tran = abs( <lfs_temp>-end_credit_tran ).
    ENDLOOP.

    SORT lt_result BY bp ASCENDING.
    " 6. Apply sorting
    DATA(sort_order) = VALUE abap_sortorder_tab(
      FOR sort_element IN io_request->get_sort_elements( )
      ( name = sort_element-element_name descending = sort_element-descending ) ).
    IF sort_order IS NOT INITIAL.
      SORT lt_result BY (sort_order).
    ENDIF.

    " 7. Apply paging
    DATA(lv_total_records) = lines( lt_result ).

    DATA(lo_paging) = io_request->get_paging( ).
    IF lo_paging IS BOUND.
      DATA(top) = lo_paging->get_page_size( ).
      IF top < 0. " -1 means all records
        top = lv_total_records.
      ENDIF.
      DATA(skip) = lo_paging->get_offset( ).

      IF skip >= lv_total_records.
        CLEAR lt_result. " Offset is beyond the total number of records
      ELSEIF top = 0.
        CLEAR lt_result. " No records requested
      ELSE.
        " Calculate the actual range to keep
        DATA(lv_start_index) = skip + 1. " ABAP uses 1-based indexing
        DATA(lv_end_index) = skip + top.

        " Ensure end index doesn't exceed table size
        IF lv_end_index > lv_total_records.
          lv_end_index = lv_total_records.
        ENDIF.

        " Create a new table with only the required records
        DATA: lt_paged_result LIKE lt_result.
        CLEAR lt_paged_result.

        " Copy only the required records
        DATA(lv_index) = lv_start_index.
        WHILE lv_index <= lv_end_index.
          APPEND lt_result[ lv_index ] TO lt_paged_result.
          lv_index = lv_index + 1.
        ENDWHILE.

        lt_result = lt_paged_result.
      ENDIF.
    ENDIF.
    " 6. Set response
    IF io_request->is_data_requested( ).
      io_response->set_data( lt_result ).
    ENDIF.
    IF io_request->is_total_numb_of_rec_requested( ).
      io_response->set_total_number_of_records( lines( lt_result ) ).
    ENDIF.
  ENDMETHOD.
ENDCLASS.
