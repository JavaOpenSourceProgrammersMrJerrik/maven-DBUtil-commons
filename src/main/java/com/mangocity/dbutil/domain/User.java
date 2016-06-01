package com.mangocity.dbutil.domain;

import java.io.Serializable;

import com.google.common.base.Objects;

public class User implements Serializable {

	private static final long serialVersionUID = -3089926791011388191L;

	private Integer userId;

	private String userName;

	private String desc;

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("userId", userId)
				.add("userName", userName).add("desc", desc).toString();
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

}
