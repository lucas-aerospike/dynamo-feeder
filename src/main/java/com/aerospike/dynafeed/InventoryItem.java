package com.aerospike.dynafeed;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;

import java.math.BigDecimal;

@DynamoDbBean
public class InventoryItem {

    /**
     * The name of the GSI that supports queries by {@code listingUser}.
     * The index itself is created in the AWS console; this constant
     * is referenced by the secondary-key annotations below to declare
     * the bean's participation in that index to the Enhanced Client.
     */
    public static final String LISTING_USER_INDEX = "listingUser-index";

    private String itemId;        // UUIDv4 in canonical string form
    private long listingUser;
    private String brand;
    private String category;
    private String subcategory;
    private String style;
    private String gender;        // "M", "W", "U"
    private String size;          // polymorphic: "6", "XL", "32x34"
    private String year;          // year of manufacture as string
    private String color;
    private String condition;     // "mint/unopened", "very good", "good", "fair", "poor"
    private BigDecimal price;     // USD
    private String title;         // derived at creation

    // Required: no-arg constructor
    public InventoryItem() {
    }

    @DynamoDbPartitionKey
    @DynamoDbSecondarySortKey(indexNames = LISTING_USER_INDEX)
    @DynamoDbAttribute("itemId")
    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = LISTING_USER_INDEX)
    @DynamoDbAttribute("listingUser")
    public long getListingUser() {
        return listingUser;
    }

    public void setListingUser(long listingUser) {
        this.listingUser = listingUser;
    }

    @DynamoDbAttribute("brand")
    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    @DynamoDbAttribute("category")
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @DynamoDbAttribute("subcategory")
    public String getSubcategory() {
        return subcategory;
    }

    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    @DynamoDbAttribute("style")
    public String getStyle() {
        return style;
    }

    public void setStyle(String style) {
        this.style = style;
    }

    @DynamoDbAttribute("gender")
    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @DynamoDbAttribute("size")
    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    @DynamoDbAttribute("year")
    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @DynamoDbAttribute("color")
    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    @DynamoDbAttribute("condition")
    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    @DynamoDbAttribute("price")
    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    @DynamoDbAttribute("title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
