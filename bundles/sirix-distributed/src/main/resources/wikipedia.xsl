<xsl:stylesheet version="2.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema"
	exclude-result-prefixes="xs">

	<xsl:output method="xml" indent="no" omit-xml-declaration="yes" />
	<xsl:strip-space elements="*" />

	<xsl:template match="/">
		<xsl:copy>
			<xsl:for-each-group select="descendant-or-self::page"
				group-by="concat(id, revision/timestamp)">
				<page>
					<xsl:copy-of select="* except revision" />
					<!--
						<xsl:copy-of select="./node()[not(local-name() = 'revision')]" />
					-->
					<xsl:for-each-group select="current-group()/revision"
						group-by="xs:dateTime(timestamp)">
						<xsl:apply-templates select="current-group()" />
					</xsl:for-each-group>
				</page>
			</xsl:for-each-group>
		</xsl:copy>
	</xsl:template>

	<xsl:template match="revision">
		<xsl:copy-of select="." />
	</xsl:template>

</xsl:stylesheet>