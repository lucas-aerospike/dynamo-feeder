package com.aerospike.dynafeed;


import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;


@DynamoDbBean
public class User
{
    private long userId;
    private String firstName;
    private String lastName;
    private String email;
    private String phoneNumber;      // US format, e.g. "+1-415-555-0142"
    private String addressLine1;
    private String addressLine2;     // nullable
    private String city;
    private String state;            // 2-letter USPS code, e.g. "CA"
    private String zip;              // 5-digit or ZIP+4, stored as string to preserve leading zeros


    public User( )
    {
    }


    @DynamoDbPartitionKey
    @DynamoDbAttribute("userId")
    public long getUserId( )
    {
        return userId;
    }


    public void setUserId(long userId)
    {
        this.userId = userId;
    }


    @DynamoDbAttribute("firstName")
    public String getFirstName( )
    {
        return firstName;
    }


    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }


    @DynamoDbAttribute("lastName")
    public String getLastName( )
    {
        return lastName;
    }


    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }


    @DynamoDbAttribute("email")
    public String getEmail( )
    {
        return email;
    }


    public void setEmail(String email)
    {
        this.email = email;
    }


    @DynamoDbAttribute("phoneNumber")
    public String getPhoneNumber( )
    {
        return phoneNumber;
    }


    public void setPhoneNumber(String phoneNumber)
    {
        this.phoneNumber = phoneNumber;
    }


    @DynamoDbAttribute("addressLine1")
    public String getAddressLine1( )
    {
        return addressLine1;
    }


    public void setAddressLine1(String addressLine1)
    {
        this.addressLine1 = addressLine1;
    }


    @DynamoDbAttribute("addressLine2")
    public String getAddressLine2( )
    {
        return addressLine2;
    }


    public void setAddressLine2(String addressLine2)
    {
        this.addressLine2 = addressLine2;
    }


    @DynamoDbAttribute("city")
    public String getCity( )
    {
        return city;
    }


    public void setCity(String city)
    {
        this.city = city;
    }


    @DynamoDbAttribute("state")
    public String getState( )
    {
        return state;
    }


    public void setState(String state)
    {
        this.state = state;
    }


    @DynamoDbAttribute("zip")
    public String getZip( )
    {
        return zip;
    }


    public void setZip(String zip)
    {
        this.zip = zip;
    }
}
