<?xml version='1.0'?>
<!-- vim:set sts=2 shiftwidth=2 syntax=xml: -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>


<xsl:import href="http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl"/>

<xsl:import href="settings.xsl"/>

<xsl:param name="chunk.section.depth" select="0"/>
<xsl:param name="chunk.first.sections" select="1"/>
<xsl:param name="use.id.as.filename" select="1"/>

<!-- 
    Our ulink stylesheet omits @url part if content was specified
-->
<xsl:template match="ulink">
  <xsl:variable name="content">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:if test="$content = ''">
    <xsl:text>: </xsl:text>
  </xsl:if>
  <xsl:if test="$content != ''">
    <xsl:value-of select="$content" />
  </xsl:if>
  <xsl:if test="$content = ''">
    <xsl:apply-templates mode="italic" select="@url" />
  </xsl:if>
</xsl:template>

<xsl:template match="informalexample|screen|programlisting">
  <xsl:text>.nf&#10;</xsl:text>
  <xsl:apply-templates/>
  <xsl:text>.fi&#10;</xsl:text>
</xsl:template>

<xsl:template match="para|simpara|remark" mode="list">
  <xsl:variable name="foo">
    <xsl:apply-templates/>
  </xsl:variable>
  <xsl:choose match="node()">
    <!-- Don't normalize-space() for verbatim paragraphs        -->
    <xsl:when test="informalexample|screen|programlisting">
      <xsl:value-of select="$foo"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="normalize-space($foo)"/>
      <xsl:text>&#10;</xsl:text>
    </xsl:otherwise>
  </xsl:choose>
  <xsl:text>&#10;</xsl:text>
  <xsl:if test="following-sibling::para or following-sibling::simpara or
		following-sibling::remark">
    <!-- Make sure multiple paragraphs within a list item don't -->
    <!-- merge together.                                        -->
    <xsl:text>&#10;</xsl:text>
  </xsl:if>
</xsl:template>

<xsl:template match="refsect3">
  <xsl:text>&#10;.SS "</xsl:text>
  <xsl:value-of select="title[1]"/>
  <xsl:text>"&#10;</xsl:text>
  <xsl:apply-templates/>
</xsl:template>


</xsl:stylesheet>
