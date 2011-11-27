//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.11.27 at 11:30:16 PM GMT+05:30 
//


package org.apache.airavat.entity.v0;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for late-processType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="late-processType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="late-input" type="{}late-inputType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *       &lt;attribute name="policy" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="delay" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="delayUnit" type="{http://www.w3.org/2001/XMLSchema}string" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "late-processType", propOrder = {
    "lateInput"
})
public class LateProcessType {

    @XmlElement(name = "late-input")
    protected List<LateInputType> lateInput;
    @XmlAttribute
    protected String policy;
    @XmlAttribute
    protected String delay;
    @XmlAttribute
    protected String delayUnit;

    /**
     * Gets the value of the lateInput property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the lateInput property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getLateInput().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link LateInputType }
     * 
     * 
     */
    public List<LateInputType> getLateInput() {
        if (lateInput == null) {
            lateInput = new ArrayList<LateInputType>();
        }
        return this.lateInput;
    }

    /**
     * Gets the value of the policy property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPolicy() {
        return policy;
    }

    /**
     * Sets the value of the policy property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPolicy(String value) {
        this.policy = value;
    }

    /**
     * Gets the value of the delay property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDelay() {
        return delay;
    }

    /**
     * Sets the value of the delay property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDelay(String value) {
        this.delay = value;
    }

    /**
     * Gets the value of the delayUnit property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDelayUnit() {
        return delayUnit;
    }

    /**
     * Sets the value of the delayUnit property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDelayUnit(String value) {
        this.delayUnit = value;
    }

}
