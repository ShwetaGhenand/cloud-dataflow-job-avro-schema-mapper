package com.example.common;

public class Constants {

  private Constants() {

  }

  public static final String JOIN_TABLE_QUERY = "SELECT item.prodcut_id,product_description,product_item_number,product_name,product_quantity  ,product_brand  ,product_category    FROM item inner join productdetails on item.prodcut_id=productdetails.prodcut_id;";
}
