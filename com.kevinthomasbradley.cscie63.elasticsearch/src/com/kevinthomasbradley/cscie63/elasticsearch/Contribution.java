package com.kevinthomasbradley.cscie63.elasticsearch;

/**
 * @author Kevin Thomas Bradley
 * @dateCreated 4-May-2015
 * @description This class represents a contribution made
 * @version 1.0
 * @codeReviewer
 */
public class Contribution {
	
	private String id;
	private String candidateName;
	private String contributorName;
	private String contributorCity;
	private String contributorStateCode;
	private String contributorState;
	private String contributorZip;
	private String contributorEmployer;
	private String contributorOccupation;
	private double	amount;
	private String date;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCandidateName() {
		return candidateName;
	}

	public void setCandidateName(String candidateName) {
		this.candidateName = candidateName;
	}

	public String getContributorName() {
		return contributorName;
	}

	public void setContributorName(String contributorName) {
		this.contributorName = contributorName;
	}

	public String getContributorCity() {
		return contributorCity;
	}

	public void setContributorCity(String contributorCity) {
		this.contributorCity = contributorCity;
	}

	public String getContributorStateCode() {
		return contributorStateCode;
	}

	public void setContributorStateCode(String contributorStateCode) {
		this.contributorStateCode = contributorStateCode;
	}

	public String getContributorState() {
		return contributorState;
	}

	public void setContributorState(String contributorState) {
		this.contributorState = contributorState;
	}

	public String getContributorZip() {
		return contributorZip;
	}

	public void setContributorZip(String contributorZip) {
		this.contributorZip = contributorZip;
	}

	public String getContributorEmployer() {
		return contributorEmployer;
	}

	public void setContributorEmployer(String contributorEmployer) {
		this.contributorEmployer = contributorEmployer;
	}

	public String getContributorOccupation() {
		return contributorOccupation;
	}

	public void setContributorOccupation(String contributorOccupation) {
		this.contributorOccupation = contributorOccupation;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
	@Override
	public String toString() {
		return this.getContributorName() + " paid $" + this.getAmount() + " to the campaign of " + this.getCandidateName();
	}
	
}
