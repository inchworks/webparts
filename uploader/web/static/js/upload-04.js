// Copyright © Rob Burke inchworks.com, 2020.

// Client-side functions to upload media files (audio, images and videos).

var nextVersion = 1;

// Upload file specified for uploading.
function uploadFile($inp, token, maxUpload, timestamp, $btnSubmit) {

    var fileName = $inp.val().split("\\").pop();
    var file = $inp[0].files[0];
    var $slide = $inp.closest(".childForm")
    var $media = $inp.closest(".media")

    // disable submit button
    $btnSubmit.prop("disabled", true);

    // show file name in form entry, as confirmation to user ..
    $inp.siblings(".upload-text").addClass("selected").html(fileName);

    // set file name and version in hidden fields, so we can match the image to the slide
    $media.children(".mediaName").val(fileName);
    $media.children(".mediaVersion").val(nextVersion);

    // clear previous status
    reset($slide);

    // check file size (rounding to nearest MB)
    var sz = (file.size + (1 << 19)) >> 20
    if (sz > maxUpload) {
         uploadRejected($slide, "This file is " + sz + " MB, " + maxUpload + " MB is allowed");
         return;
    }

    // show progress and status
    $slide.find(".upload").show();

    // upload file with AJAX
    var fd = new FormData();
    fd.append('csrf_token', token);
    fd.append('timestamp', timestamp);
    fd.append('version', nextVersion++);
    fd.append('media', file);

    $.ajax({
        url: '/upload',  
        type: 'POST',
        data: fd,
        dataType: 'json',
        success:function(reply, rqStatus, jq){ uploaded($slide, reply, rqStatus) },
        error:function(jq, rqStatus, error){ uploadFailed($slide, rqStatus, error) },
        cache: false,
        contentType: false,
        processData: false,
        xhr: function() { return xhrWithProgress($slide); }
    });
}

// XHR object with progress monitoring.
function xhrWithProgress($slide) {
    var xhr = $.ajaxSettings.xhr();
    var $p = $slide.find(".progress-bar");
    xhr.upload.onprogress = function (e) {
        if (e.lengthComputable) {
            var percent = (e.loaded / e.total) * 100;
            $p.width(percent + '%');
        }
    };
    return xhr;	
}

// Event handler for upload request done.
function uploaded($slide, reply, rqStatus) {
    var $alert = $slide.find(".upload-status")
    if (reply.error == "")
        setStatus($alert, "uploaded", "alert-success");

    else {
        // rejected by server - discard filename
        setStatus($alert, reply.error, "alert-danger");
        $slide.find(".mediaName").val("");
    }

    // re-enable submit button
    $("#submit").prop("disabled", false);
}

// Event handler for upload failed.
function uploadFailed($slide, rqStatus, error) {
    var $alert = $slide.find(".upload-status")
    setStatus($alert, rqStatus + " : " + error, "alert-danger")

    // discard filename, so client doesn't claim to have uploaded it
    $slide.find(".mediaName").val("");

    // re-enable submit button
    $("#submit").prop("disabled", false);
}

// Upload rejected.
function uploadRejected($slide, error) {
    var $badFile = $slide.find(".bad-file");
    $badFile.text(error);
    $badFile.show();

    // discard filename, so client doesn't claim to have uploaded it
    $slide.find(".mediaName").val("");

    // re-enable submit button
    $("#submit").prop("disabled", false);
}

// Reset upload bar and status fields.
function reset($slide) {
    $slide.find(".upload").hide();
    $slide.find(".progress-bar").width(0);

    var $alert = $slide.find(".upload-status")
    $alert.text("");
    $alert.removeClass("alert-success alert-danger");

    var $badFile = $slide.find(".bad-file");
    $badFile.text("");
    $badFile.hide();
}

// Set upload status.
function setStatus($alert, status, highlight) {
    $alert.text(status);
    $alert.addClass(highlight);
}
