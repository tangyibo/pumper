package com.gitee.inrgihc.pumper.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnDescription {

  private String fieldName;
  private String fieldType;
  private int dateType;

}
