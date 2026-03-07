package io.sirix.rest.crud

import io.vertx.ext.web.handler.HttpException

/**
 * Validates path parameters (database names, resource names) to prevent
 * directory traversal attacks before they are used in [java.nio.file.Path.resolve].
 */
object PathValidation {

    private const val MAX_PATH_PARAM_LENGTH = 255

    /**
     * Validates that a path parameter is safe for use in [java.nio.file.Path.resolve].
     *
     * @param name the path parameter value to validate
     * @param paramName the name of the parameter (for error messages, e.g. "database" or "resource")
     * @throws HttpException with status 400 if the name is invalid
     */
    @JvmStatic
    fun validatePathParam(name: String, paramName: String) {
        if (name.isBlank()) {
            throw HttpException(400, "Path parameter '$paramName' must not be blank.")
        }

        if (name.length > MAX_PATH_PARAM_LENGTH) {
            throw HttpException(400, "Path parameter '$paramName' exceeds maximum length of $MAX_PATH_PARAM_LENGTH characters.")
        }

        if (name.contains("..")) {
            throw HttpException(400, "Path parameter '$paramName' must not contain '..'.")
        }

        if (name.contains('/')) {
            throw HttpException(400, "Path parameter '$paramName' must not contain '/'.")
        }

        if (name.contains('\\')) {
            throw HttpException(400, "Path parameter '$paramName' must not contain '\\'.")
        }

        if (name.indexOf('\u0000') >= 0) {
            throw HttpException(400, "Path parameter '$paramName' must not contain null bytes.")
        }
    }
}
