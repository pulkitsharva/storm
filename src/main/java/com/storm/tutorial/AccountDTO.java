package com.storm.tutorial;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class AccountDTO {

  private Boolean shouldFailAtAccount;
  private Boolean shouldFailAtSms;
  private String status;


}
