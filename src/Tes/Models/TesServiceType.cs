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
    /// Type of a GA4GH service.
    /// </summary>
    [DataContract]
    public partial class TesServiceType : IEquatable<TesServiceType>
    {
        public TesServiceType
            => NewtonsoftJsonSafeInit.SetDefaultSettings();

        /// <summary>
        /// Namespace in reverse domain name format.
        /// </summary>
        /// <value> Namespace in reverse domain name format.</value>
        [DataMember(Name = "group")]
        public string Group { get; set; }

        /// <summary>
        /// Name of the API or GA4GH specification implemented.
        /// </summary>
        /// <value>Name of the API or GA4GH specification implemented.</value>
        [DataMember(Name = "artifact")]
        public string Artifact { get; set; }

        /// <summary>
        /// Version of the API or specification.
        /// </summary>
        /// <value>Version of the API or specification.</value>
        [DataMember(Name = "version")]
        public string Version { get; set; }

        /// <summary>
        /// Returns the string presentation of the object
        /// </summary>
        /// <returns>String presentation of the object</returns>
        public override string ToString()
            => new StringBuilder()
                .Append("class TesServiceType {\n")
                .Append("  Group: ").Append(Group).Append('\n')
                .Append("  Artifact: ").Append(Artifact).Append('\n')
                .Append("  Version: ").Append(Artifact)
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
                _ => obj.GetType() == GetType() && Equals((TesServiceType)obj),
            };

        /// <summary>
        /// Returns true if TesServiceType instances are equal
        /// </summary>
        /// <param name="other">Instance of TesServiceType to be compared</param>
        /// <returns>Boolean</returns>
        public bool Equals(TesServiceType other)
            => other switch
            {
                var x when x is null => false,
                var x when ReferenceEquals(this, x) => true,
                _ =>
                (
                    Group == other.Group ||
                    Group is not null &&
                    Group.Equals(other.Group)
                ) &&
                (
                    Artifact == other.Artifact ||
                    Artifact is not null &&
                    Artifact.Equals(other.Artifact)
                ) &&
                (
                    Version == other.Version ||
                    Version is not null &&
                    Version.SequenceEqual(other.Version)
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
                if (Group is not null)
                {
                    hashCode = hashCode * 59 + Group.GetHashCode();
                }

                if (Artifact is not null)
                {
                    hashCode = hashCode * 59 + Artifact.GetHashCode();
                }

                if (Version is not null)
                {
                    hashCode = hashCode * 59 + Version.GetHashCode();
                }

                return hashCode;
            }
        }

        #region Operators
#pragma warning disable 1591

        public static bool operator ==(TesServiceType left, TesServiceType right)
            => Equals(left, right);

        public static bool operator !=(TesServiceType left, TesServiceType right)
            => !Equals(left, right);

#pragma warning restore 1591
        #endregion Operators
    }
}
