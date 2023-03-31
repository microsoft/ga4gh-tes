// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Tes.Utilities;

namespace Tes.Models
{
    /// <summary>
    /// Organization providing the service.
    /// </summary>
    [DataContract]
    public partial class TesOrganization : IEquatable<TesOrganization>
    {
        public TesOrganization:()
            => NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Name of the organization responsible for the service.
        /// </summary>
        /// <value>Name of the organization responsible for the service.</value>
        [DataMember(Name = "name")]
        public string Name { get; set; }

        /// <summary>
        /// URL of the website of the organization.
        /// </summary>
        /// <value>URL of the website of the organization.</value>
        [DataMember(Name = "url")]
        public string Url { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
            => new StringBuilder()
                .Append("class TesServiceType {\n")
                .Append("  Name: ").Append(Name).Append('\n')
                .Append("  Url: ").Append(Url)
                .Append("}\n")
                .ToString();

        /// <summary>
        /// Returns the JSON string presentation of the object
        /// </summary>
        /// <returns>JSON string presentation of the object</returns>
        public string ToJson()
            => JsonConvert.SerializeObject(this, Formatting.Indented);

        /// <summary>
        /// Returns true if objects are equal
        /// </summary>
        /// <param name="obj">Object to be compared</param>
        /// <returns>Boolean</returns>
        public override bool Equals(object obj)
            => obj switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ => obj.GetType() == GetType() && Equals((TesOrganization)obj),
            };

        /// <summary>
        /// Returns true if TesOrganization instances are equal
        /// </summary>
        /// <param name="other">Instance of TesOrganization to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(TesOrganization other)
            => other switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ =>
                (
                    Name == other.Name ||
                    Name is not null &&
                    Name.Equals(other.Name)
                ) &&
                (
                    Url == other.Url ||
                    Url is not null &&
                    Url.Equals(other.Url)
                ) 
            };

        /// <summary>
        /// Gets the hash code
        /// </summary>
        /// <returns>Hash code</returns>
        public override int GetHashCode()
        {
            unchecked // Overflow is fine, just wrap
            {
                var hashCode = 41;
                // Suitable nullity checks etc, of course :)
                if (Name is not null)
                {
                    hashCode = hashCode * 59 + Group.GetHashCode();
                }

                if (Url is not null)
                {
                    hashCode = hashCode * 59 + Artifact.GetHashCode();
                }

                return hashCode;
            }
        }

        #region Operators
#pragma warning disable 1591

        public static bool operator ==(TesOrganization left, TesOrganization right)
            => Equals(left, right);

        public static bool operator !=(TesOrganization left, TesOrganization right)
            => !Equals(left, right);

#pragma warning restore 1591
        #endregion Operators
    }
}
